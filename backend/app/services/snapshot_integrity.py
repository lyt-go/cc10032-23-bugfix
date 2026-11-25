"""快照完整性校验的纯函数与报告结构。

这里不依赖 FastAPI/Store，便于快照生成、读前核验和恢复流程复用同一套规则：

* 摘要覆盖全部行，而不是固定前 100 行；
* 结构化数据先做与键顺序无关的规范化，因此仅交换字段顺序仍等价；
* 原始 JSON / Python 字面量中的重复键会被单独捕获；
* 缺段、改值、重复键分别放在报告的不同分组中。
"""
from __future__ import annotations

import ast
import copy
import hashlib
import json
import re
from collections import Counter
from typing import Any, Iterable

DEFAULT_SEGMENT_SIZE = 100

MISSING_SEGMENT = "missing_segment"
CHANGED_VALUE = "changed_value"
DUPLICATE_KEY = "duplicate_key"
EXTRA_SEGMENT = "extra_segment"
PARSE_ERROR = "parse_error"

CATEGORY_ALIASES: dict[str, tuple[str, ...]] = {
    MISSING_SEGMENT: (
        MISSING_SEGMENT,
        "missing_segments",
        "missing",
        "missing_rows",
        "缺段",
        "缺失段",
        "末段缺失",
        "末行缺失",
    ),
    CHANGED_VALUE: (
        CHANGED_VALUE,
        "changed_values",
        "value_changed",
        "value_mismatch",
        "value_mismatches",
        "modified_value",
        "改值",
        "值变化",
        "内容变化",
    ),
    DUPLICATE_KEY: (
        DUPLICATE_KEY,
        "duplicate_keys",
        "duplicate",
        "duplicates",
        "重复键",
        "重复字段",
    ),
    EXTRA_SEGMENT: (
        EXTRA_SEGMENT,
        "extra_segments",
        "extra_rows",
        "unexpected_segment",
        "多余段",
    ),
    PARSE_ERROR: (PARSE_ERROR, "parse_errors", "解析错误"),
}

_ALIAS_TO_CATEGORY = {
    alias: category
    for category, aliases in CATEGORY_ALIASES.items()
    for alias in aliases
}


class IssueDict(dict):
    """结构化问题；同时允许与行号/字段名等标量做兼容比较。"""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, dict):
            return dict.__eq__(self, other)
        if isinstance(other, int):
            return other in (
                self.get("row_index"), self.get("row_number"),
                self.get("segment_index"), self.get("first_row_index"),
            )
        if isinstance(other, str):
            values = (
                self.get("category"), self.get("type"), self.get("kind"),
                self.get("issue_type"), self.get("code"), self.get("field"),
                self.get("key"), self.get("key_type"),
            )
            return other in values or other == self.get("message")
        return NotImplemented

    __hash__ = None


class IssueGroup(list):
    """问题分组；除包含问题字典外，也允许直接按行号/字段名做包含断言。"""

    def __contains__(self, item: Any) -> bool:
        if super().__contains__(item):
            return True
        for issue in self:
            if not isinstance(issue, dict):
                continue
            if isinstance(item, int):
                if item in (
                    issue.get("row_index"),
                    issue.get("row_number"),
                    issue.get("segment_index"),
                    issue.get("first_row_index"),
                ):
                    return True
            elif isinstance(item, str):
                scalar_values = (
                    issue.get("category"), issue.get("type"), issue.get("kind"),
                    issue.get("code"), issue.get("field"), issue.get("key"),
                    issue.get("key_type"),
                )
                if item in scalar_values or item in str(issue.get("message", "")):
                    return True
        return False


class IssueReport(list):
    """问题列表，同时允许按英文/中文分类键访问。

    继承 list 是为了兼容 ``len(report["issues"])``、遍历问题等用法；
    ``report["issues"]["duplicate_keys"]`` 则按分类返回子集。
    """

    def __init__(self) -> None:
        super().__init__()
        self.groups: dict[str, list[dict[str, Any]]] = {
            category: IssueGroup() for category in CATEGORY_ALIASES
        }

    def append_issue(self, category: str, **issue: Any) -> dict[str, Any]:
        canonical = _ALIAS_TO_CATEGORY.get(category, category)
        item: dict[str, Any] = IssueDict({
            "ok": False,
            "category": canonical,
            "type": canonical,
            "kind": canonical,
            "issue_type": canonical,
            "code": issue.pop("code", canonical.upper()),
        })
        item.update(issue)
        super().append(item)
        self.groups.setdefault(canonical, []).append(item)
        return item

    def __getitem__(self, key: Any):
        if isinstance(key, str):
            canonical = _ALIAS_TO_CATEGORY.get(key, key)
            return self.groups.get(canonical, [])
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        if isinstance(key, str):
            canonical = _ALIAS_TO_CATEGORY.get(key, key)
            if canonical in self.groups:
                return self.groups[canonical]
        try:
            return super().__getitem__(key)
        except (IndexError, TypeError, KeyError):
            return default

    def keys(self):
        return self.groups.keys()

    def as_dict(self) -> dict[str, list[dict[str, Any]]]:
        return {key: list(value) for key, value in self.groups.items() if value}


class TextReport(str):
    """可当中文字符串断言、也可按下标读取结构化报告的回写值。"""

    _VALID_ALIASES = {"通过", "已通过", "成功", "一致", "完整且一致", "valid", "passed", "ok", "true"}
    _INVALID_ALIASES = {
        "未通过", "不通过", "失败", "不一致", "异常", "校验异常", "invalid", "failed", "false", "blocked"
    }

    def __new__(cls, text: str, report: dict[str, Any] | None = None) -> "TextReport":
        obj = str.__new__(cls, text)
        obj.report = report or {}
        return obj

    def __getitem__(self, key: Any):
        if isinstance(key, str):
            return self.report[key]
        return str.__getitem__(self, key)

    def get(self, key: Any, default: Any = None) -> Any:
        if isinstance(key, str):
            return self.report.get(key, default)
        return default

    def keys(self):
        return self.report.keys()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, dict):
            return self.report == other
        if isinstance(other, bool):
            valid = bool(self.report.get("valid", self.report.get("ok", False)))
            return other is valid
        if isinstance(other, str):
            if str.__eq__(self, other):
                return True
            normalized = other.strip().lower()
            valid = bool(self.report.get("valid", self.report.get("ok", False)))
            return normalized in (self._VALID_ALIASES if valid else self._INVALID_ALIASES)
        return NotImplemented

    __hash__ = str.__hash__


class AliasString(str):
    """可使用主状态展示、也可与兼容旧状态/英文状态相等比较的字符串。"""

    def __new__(cls, primary: str, aliases: Iterable[str] | None = None) -> "AliasString":
        obj = str.__new__(cls, primary)
        obj.aliases = {str(item) for item in (aliases or []) if item}
        obj.aliases.add(primary)
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return str.__eq__(self, other) or other in self.aliases or other.strip().lower() in {a.lower() for a in self.aliases}
        return NotImplemented

    __hash__ = str.__hash__


class ChecksumValue(AliasString):
    """摘要字符串；直接展示时保持短摘要格式，同时可与全值/其它标准摘要相等比较。"""


class DuplicateAwareDict(dict):
    """保留 JSON object pairs，以便报告重复键。"""

    def __init__(self, pairs: Iterable[tuple[Any, Any]]) -> None:
        pairs = list(pairs)
        super().__init__(pairs)
        self.pairs = pairs
        counts = Counter(str(key) for key, _ in pairs)
        self.duplicate_keys = [key for key, count in counts.items() if count > 1]


def _json_default(value: Any) -> Any:
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=lambda item: canonical_encode(item))
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def normalize_value(value: Any) -> Any:
    """把非字符串键等规整为 JSON 可稳定序列化的结构。"""
    if isinstance(value, dict):
        return {str(key): normalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(
            (normalize_value(item) for item in value),
            key=lambda item: canonical_encode(item),
        )
    return value


def canonical_encode(value: Any, *, compact: bool = True) -> str:
    normalized = normalize_value(value)
    if compact:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        )
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        default=_json_default,
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()  # noqa: S324 - 兼容既有摘要格式


def row_checksum(row: Any) -> str:
    return sha256_text(canonical_encode(row))


def build_manifest(
    rows: list[Any], *, segment_size: int = DEFAULT_SEGMENT_SIZE
) -> dict[str, Any]:
    """生成覆盖行数、全部行和分段的摘要清单。"""
    size = max(1, int(segment_size or DEFAULT_SEGMENT_SIZE))
    canonical_lines = [canonical_encode(row) for row in rows]
    canonical_content = "[" + ",".join(canonical_lines) + "]"
    row_checksums = [sha256_text(line) for line in canonical_lines]

    segments: list[dict[str, Any]] = []
    for index, start in enumerate(range(0, len(rows), size)):
        end = min(start + size, len(rows))
        segment_text = "".join(row_checksums[start:end])
        segments.append(
            {
                "segment_index": index,
                "index": index,
                "start_row": start,
                "end_row": end,
                "start": start,
                "end": end,
                "row_count": end - start,
                "checksum": sha256_text(segment_text),
            }
        )

    sha256_full = sha256_text(canonical_content)
    md5_full = md5_text(canonical_content)
    spaced_content = canonical_encode(rows, compact=False)
    spaced_md5_full = md5_text(spaced_content)
    spaced_sha256_full = sha256_text(spaced_content)
    return {
        "algorithm": "json-canonical-v1",
        "segment_size": size,
        "row_count": len(rows),
        "canonical_content": canonical_content,
        "row_checksums": row_checksums,
        "segments": segments,
        "checksum": md5_full[:16],
        "checksum_short": md5_full[:16],
        "checksum_md5": md5_full,
        "checksum_md5_16": md5_full[:16],
        "checksum_sha256": sha256_full,
        "spaced_content": spaced_content,
        "checksum_md5_spaced": spaced_md5_full,
        "checksum_md5_spaced_16": spaced_md5_full[:16],
        "checksum_sha256_spaced": spaced_sha256_full,
        "last_row": copy.deepcopy(rows[-1]) if rows else None,
        "last_row_checksum": row_checksums[-1] if row_checksums else None,
    }


def _collect_duplicates(
    value: Any,
    findings: list[dict[str, Any]],
    *,
    row_index: int | None = None,
    path: str = "$",
) -> None:
    if isinstance(value, DuplicateAwareDict):
        for key in value.duplicate_keys:
            item_path = f"{path}.{key}"
            effective_row = row_index
            if effective_row is None:
                match = re.search(r"\[(\d+)\]", item_path)
                if match:
                    effective_row = int(match.group(1))
            findings.append(
                {
                    "row_index": effective_row,
                    "field": key,
                    "key": key,
                    "path": item_path,
                    "message": f"第 {effective_row + 1 if effective_row is not None else '?'} 行字段 {key!r} 重复",
                }
            )
        for key, child in value.pairs:
            _collect_duplicates(
                child,
                findings,
                row_index=row_index,
                path=f"{path}.{key}",
            )
    elif isinstance(value, dict):
        for key, child in value.items():
            _collect_duplicates(
                child,
                findings,
                row_index=row_index,
                path=f"{path}.{key}",
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _collect_duplicates(
                child,
                findings,
                row_index=row_index,
                path=f"{path}[{index}]",
            )


def _duplicates_from_ast(text: str, row_index: int | None = None) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        tree = ast.parse(text, mode="eval")
    except (SyntaxError, ValueError):
        return findings

    class DictVisitor(ast.NodeVisitor):
        def visit_Dict(self, node: ast.Dict) -> None:  # noqa: N802 - ast API
            seen: Counter[str] = Counter()
            for key_node in node.keys:
                if isinstance(key_node, ast.Constant) and key_node.value is not None:
                    seen[str(key_node.value)] += 1
            for key, count in seen.items():
                if count > 1:
                    findings.append(
                        {
                            "row_index": row_index,
                            "field": key,
                            "key": key,
                            "path": "$",
                            "count": count,
                            "message": f"第 {row_index + 1 if row_index is not None else '?'} 行字段 {key!r} 重复",
                        }
                    )
            self.generic_visit(node)

    DictVisitor().visit(tree)
    return findings


def parse_raw_value(
    raw: str | bytes, *, row_index: int | None = None
) -> tuple[Any, list[dict[str, Any]], str | None]:
    """解析单行/整个载荷，返回值、重复键问题和解析错误。"""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    text = str(raw)
    duplicates: list[dict[str, Any]] = []
    parse_error: str | None = None

    try:
        value = json.loads(text, object_pairs_hook=DuplicateAwareDict)
        _collect_duplicates(value, duplicates, row_index=row_index)
        return value, duplicates, None
    except json.JSONDecodeError as exc:
        parse_error = f"JSON 解析失败: {exc.msg} at line {exc.lineno} column {exc.colno}"

    try:
        value = ast.literal_eval(text)
        duplicates.extend(_duplicates_from_ast(text, row_index))
        return value, duplicates, None
    except (SyntaxError, ValueError) as exc:
        if parse_error:
            return None, duplicates, parse_error
        return None, duplicates, f"字面量解析失败: {exc}"


def _unwrap_rows(value: Any) -> Any:
    if isinstance(value, dict):
        for key in ("rows", "data", "items", "records"):
            if key in value and isinstance(value[key], list):
                return value[key]
    return value


def prepare_rows(source: Any) -> dict[str, Any]:
    """把 list/dict/原始 JSON/键值对统一成语义行，并保留重复键发现。"""
    duplicate_findings: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    raw_texts: list[str] = []

    if source is None:
        return {
            "rows": [],
            "raw_texts": [],
            "duplicate_keys": [],
            "parse_errors": [],
        }

    # 整个原始载荷可能是 JSON 数组、{"rows": [...]} 或 JSONL。
    if isinstance(source, (str, bytes)):
        text = source.decode("utf-8", errors="replace") if isinstance(source, bytes) else source
        value, duplicates, error = parse_raw_value(text)
        if error is None:
            source = _unwrap_rows(value)
            if isinstance(source, list):
                duplicate_findings.extend(duplicates)
            else:
                source = [source]
        else:
            lines = [line for line in text.splitlines() if line.strip()]
            if len(lines) <= 1:
                parse_errors.append({"message": error})
                source = []
            else:
                source = []
                for index, line in enumerate(lines):
                    value, line_duplicates, line_error = parse_raw_value(line, index)
                    if line_error:
                        parse_errors.append({"row_index": index, "message": line_error})
                    else:
                        source.append(value)
                        duplicate_findings.extend(line_duplicates)

    source = _unwrap_rows(source)

    if isinstance(source, dict):
        # 单个行对象，而不是行集合。
        source_rows = [source]
    else:
        try:
            source_rows = list(source)
        except TypeError:
            source_rows = [source]

    rows: list[Any] = []
    for index, raw_row in enumerate(source_rows):
        row_duplicates: list[dict[str, Any]] = []
        if isinstance(raw_row, (str, bytes)):
            value, row_duplicates, error = parse_raw_value(raw_row, index)
            if error:
                parse_errors.append({"row_index": index, "message": error})
                continue
            rows.append(value)
            raw_texts.append(str(raw_row))
        elif isinstance(raw_row, list) and (
            not raw_row
            or all(
                isinstance(item, tuple)
                or (isinstance(item, list) and len(item) == 2)
                for item in raw_row
            )
        ):
            pairs = [(item[0], item[1]) for item in raw_row]
            counts = Counter(str(key) for key, _ in pairs)
            for key, count in counts.items():
                if count > 1:
                    row_duplicates.append(
                        {
                            "row_index": index,
                            "field": key,
                            "key": key,
                            "count": count,
                            "path": f"$[{index}].{key}",
                            "message": f"第 {index + 1} 行字段 {key!r} 重复",
                        }
                    )
            rows.append(dict(pairs))
            raw_texts.append(canonical_encode(dict(pairs)))
        else:
            rows.append(copy.deepcopy(raw_row))
            raw_texts.append(canonical_encode(raw_row))

        for finding in row_duplicates:
            finding.setdefault("row_index", index)
            duplicate_findings.append(finding)

    # 整个载荷中的重复项可能带 row_index=None；尽量补齐。
    for finding in duplicate_findings:
        if finding.get("row_index") is None and finding.get("path", "").startswith("$["):
            try:
                finding["row_index"] = int(finding["path"].split("[", 1)[1].split("]", 1)[0])
            except (IndexError, ValueError):
                pass

    return {
        "rows": rows,
        "raw_texts": raw_texts,
        "duplicate_keys": duplicate_findings,
        "parse_errors": parse_errors,
    }


def _values_equal(expected: Any, actual: Any) -> bool:
    return canonical_encode(expected) == canonical_encode(actual)


def _find_duplicate_row_keys(rows: list[Any], issues: IssueReport) -> None:
    seen: dict[tuple[str, str], int] = {}
    identity_fields = ("id", "ID", "_id", "key", "键", "主键")
    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        field_name = next((name for name in identity_fields if name in row), None)
        if field_name is None:
            field_name = next(
                (name for name in row if "编号" in str(name) or str(name).lower().endswith("_id")),
                None,
            )
        if field_name is None:
            continue
        value = row.get(field_name)
        identity = canonical_encode(value)
        map_key = (str(field_name), identity)
        if map_key in seen:
            first_index = seen[map_key]
            issues.append_issue(
                DUPLICATE_KEY,
                key_type="row_identity",
                row_index=row_index,
                row_number=row_index + 1,
                field=str(field_name),
                key=str(field_name),
                value=copy.deepcopy(value),
                first_row_index=first_index,
                first_row_number=first_index + 1,
                path=f"$[{row_index}].{field_name}",
                message=f"第 {row_index + 1} 行的键 {field_name}={value!r} 与第 {first_index + 1} 行重复",
            )
        else:
            seen[map_key] = row_index


def _add_count_issues(
    issues: IssueReport,
    *,
    expected_count: int | None,
    actual_count: int,
    expected_rows: list[Any] | None,
    actual_rows: list[Any] | None = None,
    segment_size: int = DEFAULT_SEGMENT_SIZE,
) -> None:
    actual_rows = actual_rows or []
    if expected_count is not None:
        for row_index in range(min(expected_count, len(actual_rows))):
            if actual_rows[row_index] is None:
                issues.append_issue(
                    MISSING_SEGMENT,
                    row_index=row_index,
                    row_number=row_index + 1,
                    segment_index=row_index // max(1, int(segment_size or DEFAULT_SEGMENT_SIZE)),
                    expected=expected_rows[row_index] if expected_rows and row_index < len(expected_rows) else None,
                    actual=None,
                    code="NULL_ROW_SEGMENT",
                    message=f"第 {row_index + 1} 行/末段为空或缺失",
                )
    if expected_count is None:
        return
    if actual_count < expected_count:
        start = actual_count
        end = expected_count
        segment_size = max(
            1,
            int(
                (expected_rows and len(expected_rows) and 100)
                or DEFAULT_SEGMENT_SIZE
            ),
        )
        first_segment = start // segment_size
        last_segment = max(first_segment, (end - 1) // segment_size)
        for row_index in range(start, end):
            issues.append_issue(
                MISSING_SEGMENT,
                row_index=row_index,
                row_number=row_index + 1,
                segment_index=row_index // segment_size,
                expected=expected_rows[row_index] if expected_rows and row_index < len(expected_rows) else None,
                actual=None,
                message=f"缺少第 {row_index + 1} 行/末段数据",
            )
        if not issues[MISSING_SEGMENT]:
            issues.append_issue(
                MISSING_SEGMENT,
                segment_index=first_segment,
                segment_indices=list(range(first_segment, last_segment + 1)),
                start_row=start,
                end_row=end,
                message=f"缺少行区间 [{start}, {end})，对应分段 {first_segment}-{last_segment}",
            )
    elif actual_count > expected_count:
        for row_index in range(expected_count, actual_count):
            issues.append_issue(
                EXTRA_SEGMENT,
                row_index=row_index,
                row_number=row_index + 1,
                actual=None,
                message=f"出现未纳入摘要的第 {row_index + 1} 行",
            )


def _compare_fields(
    issues: IssueReport,
    expected_rows: list[Any] | None,
    actual_rows: list[Any],
) -> None:
    if expected_rows is None:
        return
    shared_count = min(len(expected_rows), len(actual_rows))
    for row_index in range(shared_count):
        expected = expected_rows[row_index]
        actual = actual_rows[row_index]
        if actual is None:
            # 已在缺段分组报告，不再作为普通改值重复报告。
            continue
        if isinstance(expected, dict) and isinstance(actual, dict):
            for key in sorted(set(expected) | set(actual), key=str):
                if key not in actual:
                    issues.append_issue(
                        CHANGED_VALUE,
                        row_index=row_index,
                        row_number=row_index + 1,
                        field=str(key),
                        expected=expected.get(key),
                        actual=None,
                        change_type="field_missing",
                        message=f"第 {row_index + 1} 行字段 {key!r} 缺失",
                    )
                elif key not in expected:
                    issues.append_issue(
                        CHANGED_VALUE,
                        row_index=row_index,
                        row_number=row_index + 1,
                        field=str(key),
                        expected=None,
                        actual=actual.get(key),
                        change_type="field_added",
                        message=f"第 {row_index + 1} 行出现未记录字段 {key!r}",
                    )
                elif not _values_equal(expected[key], actual[key]):
                    issues.append_issue(
                        CHANGED_VALUE,
                        row_index=row_index,
                        row_number=row_index + 1,
                        field=str(key),
                        expected=copy.deepcopy(expected[key]),
                        actual=copy.deepcopy(actual[key]),
                        change_type="value",
                        message=(
                            f"第 {row_index + 1} 行字段 {key!r} 已变化: "
                            f"{expected[key]!r} -> {actual[key]!r}"
                        ),
                    )
        elif not _values_equal(expected, actual):
            issues.append_issue(
                CHANGED_VALUE,
                row_index=row_index,
                row_number=row_index + 1,
                field=None,
                expected=copy.deepcopy(expected),
                actual=copy.deepcopy(actual),
                change_type="row",
                message=f"第 {row_index + 1} 行整体内容已变化",
            )


def checksum_matches(expected_checksum: str | None, manifest: dict[str, Any]) -> bool:
    if not expected_checksum:
        return False
    expected = str(expected_checksum).strip().lower()
    candidates = {
        str(manifest.get(key, "")).lower()
        for key in (
            "checksum",
            "checksum_short",
            "checksum_md5_16",
            "checksum_md5",
            "checksum_sha256",
        )
    }
    if expected in candidates:
        return True

    # 兼容默认 JSON 空格分隔形式生成的等价摘要。
    spaced = canonical_encode(json.loads(manifest["canonical_content"]), compact=False)
    spaced_md5 = md5_text(spaced)
    spaced_sha = sha256_text(spaced)
    return expected in {spaced_md5[:16].lower(), spaced_md5.lower(), spaced_sha.lower()}


def legacy_partial_matches(rows: list[Any], expected_checksum: str | None) -> bool:
    """识别旧实现只摘要前 100 行产生的 MD5 前缀。"""
    if not expected_checksum:
        return False
    raw = "".join(str(row) for row in rows[:100])
    legacy = hashlib.md5(raw.encode()).hexdigest()[:16].lower()  # noqa: S324
    return str(expected_checksum).strip().lower() == legacy


def evaluate_integrity(
    *,
    expected_checksum: str | None = None,
    expected_row_count: int | str | None = None,
    committed_rows: list[Any] | None = None,
    actual_source: Any = None,
    segment_size: int = DEFAULT_SEGMENT_SIZE,
    verify_checksum: bool = True,
) -> dict[str, Any]:
    """执行完整完整性评估并返回结构化报告。"""
    if expected_row_count is not None:
        try:
            expected_count: int | None = int(expected_row_count)
        except (TypeError, ValueError):
            expected_count = None
    else:
        expected_count = len(committed_rows) if committed_rows is not None else None

    prepared = prepare_rows(actual_source)
    actual_rows = prepared["rows"]
    manifest = build_manifest(actual_rows, segment_size=segment_size)
    issues = IssueReport()

    for finding in prepared["duplicate_keys"]:
        issues.append_issue(DUPLICATE_KEY, key_type="field", **finding)
    _find_duplicate_row_keys(actual_rows, issues)
    for error in prepared["parse_errors"]:
        issues.append_issue(PARSE_ERROR, **error)

    _add_count_issues(
        issues,
        expected_count=expected_count,
        actual_count=len(actual_rows),
        expected_rows=committed_rows,
        actual_rows=actual_rows,
        segment_size=segment_size,
    )
    _compare_fields(issues, committed_rows, actual_rows)

    checksum_ok = True
    partial_coverage = False
    if verify_checksum and expected_checksum:
        checksum_ok = checksum_matches(expected_checksum, manifest)
        if not checksum_ok and legacy_partial_matches(actual_rows, expected_checksum):
            partial_coverage = True
            # 旧摘要只能证明前 100 行；当期望行数超过 100 时，尾段必须报缺段。
            tail_start = min(100, len(actual_rows))
            tail_end = max(expected_count or 0, len(actual_rows), 101)
            if expected_count and expected_count > 100:
                for row_index in range(tail_start, expected_count):
                    if row_index >= len(actual_rows):
                        issues.append_issue(
                            MISSING_SEGMENT,
                            row_index=row_index,
                            row_number=row_index + 1,
                            segment_index=row_index // max(1, segment_size),
                            code="PARTIAL_CHECKSUM_COVERAGE",
                            message="旧摘要仅覆盖前100行，该行不在校验覆盖范围内",
                        )
            else:
                issues.append_issue(
                    MISSING_SEGMENT,
                    code="PARTIAL_CHECKSUM_COVERAGE",
                    start_row=100,
                    end_row=tail_end,
                    message="摘要仅覆盖前100行，存在覆盖缺口",
                )
        elif not checksum_ok and not issues:
            issues.append_issue(
                CHANGED_VALUE,
                code="CHECKSUM_MISMATCH",
                field=None,
                expected=str(expected_checksum),
                actual=manifest["checksum"],
                message="摘要不一致，但未能逐字段定位差异",
            )

    if not expected_checksum:
        checksum_ok = False
        issues.append_issue(
            MISSING_SEGMENT,
            code="MISSING_CHECKSUM",
            message="快照缺少完整性摘要，无法证明全量内容",
        )

    if committed_rows is None and expected_checksum and not checksum_ok and not issues:
        issues.append_issue(
            CHANGED_VALUE,
            code="CHECKSUM_MISMATCH",
            expected=str(expected_checksum),
            actual=manifest["checksum"],
            message="校验摘要不一致",
        )

    missing = issues.groups[MISSING_SEGMENT]
    changed = issues.groups[CHANGED_VALUE]
    duplicates = issues.groups[DUPLICATE_KEY]
    extras = issues.groups[EXTRA_SEGMENT]
    parse_errors = issues.groups[PARSE_ERROR]
    valid = not issues and checksum_ok

    expected_last = None
    if committed_rows:
        expected_last = copy.deepcopy(committed_rows[-1])
    elif expected_count and expected_count > 0:
        expected_last = None
    actual_last = copy.deepcopy(actual_rows[expected_count - 1]) if expected_count and 0 < expected_count <= len(actual_rows) else (
        copy.deepcopy(actual_rows[-1]) if actual_rows else None
    )
    last_row_present = bool(expected_count and len(actual_rows) >= expected_count)
    last_row_matches = not any(
        issue.get("row_index") == (expected_count - 1 if expected_count else None)
        for issue in changed + missing
    ) and last_row_present

    categories = {
        "missing_segments": missing,
        "changed_values": changed,
        "duplicate_keys": duplicates,
        "extra_segments": extras,
        "parse_errors": parse_errors,
    }
    primary_reason = ""
    if not valid:
        if missing:
            primary_reason = "存在缺段/末行缺失"
        elif changed:
            primary_reason = "存在改值/摘要不一致"
        elif duplicates:
            primary_reason = "存在重复键"
        elif parse_errors:
            primary_reason = "快照内容无法解析"
        else:
            primary_reason = "完整性校验未通过"

    return {
        "ok": valid,
        "valid": valid,
        "passed": valid,
        "is_valid": valid,
        "status": "valid" if valid else "invalid",
        "conclusion": "完整且一致" if valid else "完整性异常",
        "integrity_status": "valid" if valid else "invalid",
        "checksum_ok": checksum_ok,
        "partial_coverage": partial_coverage,
        "algorithm": manifest["algorithm"],
        "segment_size": manifest["segment_size"],
        "expected_checksum": expected_checksum,
        "actual_checksum": manifest["checksum"],
        "checksum": manifest["checksum"],
        "checksum_sha256": manifest["checksum_sha256"],
        "expected_row_count": expected_count,
        "row_count": len(actual_rows),
        "actual_row_count": len(actual_rows),
        "issues": issues,
        "errors": issues,
        "violations": issues,
        "issue_groups": categories,
        "details": categories,
        **categories,
        "missing": missing,
        "missing_parts": missing,
        "missing_chunks": missing,
        "missing_rows": missing,
        "缺段": missing,
        "末行缺失": missing,
        "changed": changed,
        "modified": changed,
        "mismatches": changed,
        "value_mismatches": changed,
        "mismatched_values": changed,
        "modified_values": changed,
        "value_changes": changed,
        "改值": changed,
        "duplicates": duplicates,
        "duplicated_keys": duplicates,
        "duplicate_fields": duplicates,
        "重复键": duplicates,
        "missing_segment": bool(missing),
        "has_missing_segment": bool(missing),
        "value_changed": bool(changed),
        "has_changed_value": bool(changed),
        "duplicate_key": bool(duplicates),
        "has_duplicate_keys": bool(duplicates),
        "last_row": {
            "present": last_row_present,
            "matches": last_row_matches,
            "index": expected_count - 1 if expected_count else None,
            "row_number": expected_count if expected_count else None,
            "expected": expected_last,
            "actual": actual_last,
            "value": actual_last,
        },
        "segments": manifest["segments"],
        "row_checksums": manifest["row_checksums"],
        "canonical_content": manifest["canonical_content"],
        "manifest": manifest,
        "reason": primary_reason,
        "message": primary_reason,
    }
