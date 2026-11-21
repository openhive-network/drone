use serde_json::Value;

/// How long an account-history page may be cached, as decided by `classify_page`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HistoryPage {
    /// No operations returned; not worth caching
    Empty,
    /// The page can never change: cache it long
    Fixed,
    /// The page may still change (newest page, past-head start, reversible ops, or unparseable)
    Mutable,
}

/// Classify a `get_account_history` result for caching.
///
/// Accepts both the `condenser_api` shape (`[[seq, {"block": N, ...}], ...]`, positional params
/// `[account, start, limit, filter_low?, filter_high?]`) and the `account_history_api` shape
/// (`{"history": [...]}`, named params).  A page is `Fixed` only when the request names an
/// explicit, unfiltered `start`, the result's last sequence number equals `start`, and every
/// operation is in a block at or below `last_irreversible_block_number`.
pub fn classify_page(params: Option<&Value>, result: &Value, last_irreversible_block_number: u32) -> HistoryPage {
    let entries = match result.get("history").unwrap_or(result).as_array() {
        Some(entries) => entries,
        None => return HistoryPage::Mutable,
    };
    if entries.is_empty() {
        return HistoryPage::Empty;
    }

    let start = match params.and_then(explicit_unfiltered_start) {
        Some(start) => start,
        None => return HistoryPage::Mutable,
    };

    let last_seq = entries.last().and_then(|entry| entry.get(0)).and_then(Value::as_u64);
    if last_seq != Some(start) {
        return HistoryPage::Mutable;
    }

    let mut highest_block = 0u64;
    for entry in entries {
        match entry.pointer("/1/block").and_then(Value::as_u64) {
            Some(block) => highest_block = highest_block.max(block),
            None => return HistoryPage::Mutable,
        }
    }
    if highest_block <= last_irreversible_block_number as u64 { HistoryPage::Fixed } else { HistoryPage::Mutable }
}

/// The request's `start`, unless it is -1 (newest page), missing, or the request uses an operation
/// filter.  A filtered page's last entry is only the highest *matching* sequence number at or
/// below `start`, so it cannot prove the page is complete.
fn explicit_unfiltered_start(params: &Value) -> Option<u64> {
    let (start, filter_low, filter_high) = match params {
        Value::Array(positional) => (positional.get(1), positional.get(3), positional.get(4)),
        Value::Object(named) => (named.get("start"), named.get("operation_filter_low"), named.get("operation_filter_high")),
        _ => return None,
    };
    if is_filter_set(filter_low) || is_filter_set(filter_high) {
        return None;
    }
    start.and_then(Value::as_u64)
}

fn is_filter_set(filter: Option<&Value>) -> bool {
    match filter {
        None | Some(Value::Null) => false,
        Some(Value::Number(n)) => n.as_u64() != Some(0),
        Some(Value::String(s)) => s != "0",
        Some(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_page, HistoryPage};
    use serde_json::{json, Value};

    const LIB: u32 = 1000;

    fn condenser_result(entries: &[(u64, u64)]) -> Value {
        Value::Array(entries.iter().map(|(seq, block)| json!([seq, {"block": block, "op": ["vote", {}]}])).collect())
    }

    fn history_api_result(entries: &[(u64, u64)]) -> Value {
        json!({"history": condenser_result(entries)})
    }

    fn condenser_params(start: i64) -> Value {
        json!(["alice", start, 3])
    }

    fn history_api_params(start: i64) -> Value {
        json!({"account": "alice", "start": start, "limit": 3})
    }

    #[test]
    fn complete_irreversible_page_is_fixed() {
        let entries = [(8, 990), (9, 995), (10, 1000)];
        assert_eq!(classify_page(Some(&condenser_params(10)), &condenser_result(&entries), LIB), HistoryPage::Fixed);
        assert_eq!(classify_page(Some(&history_api_params(10)), &history_api_result(&entries), LIB), HistoryPage::Fixed);
    }

    #[test]
    fn start_past_newest_operation_is_mutable() {
        let entries = [(8, 990), (9, 995), (10, 1000)];
        assert_eq!(classify_page(Some(&condenser_params(50)), &condenser_result(&entries), LIB), HistoryPage::Mutable);
        assert_eq!(classify_page(Some(&history_api_params(50)), &history_api_result(&entries), LIB), HistoryPage::Mutable);
    }

    #[test]
    fn page_with_reversible_operation_is_mutable() {
        let entries = [(8, 990), (9, 1001), (10, 999)];
        assert_eq!(classify_page(Some(&condenser_params(10)), &condenser_result(&entries), LIB), HistoryPage::Mutable);
        assert_eq!(classify_page(Some(&history_api_params(10)), &history_api_result(&entries), LIB), HistoryPage::Mutable);
    }

    #[test]
    fn newest_page_request_is_mutable() {
        let entries = [(8, 990), (9, 995), (10, 1000)];
        assert_eq!(classify_page(Some(&condenser_params(-1)), &condenser_result(&entries), LIB), HistoryPage::Mutable);
        assert_eq!(classify_page(Some(&history_api_params(-1)), &history_api_result(&entries), LIB), HistoryPage::Mutable);
    }

    #[test]
    fn empty_page_is_empty() {
        assert_eq!(classify_page(Some(&condenser_params(10)), &condenser_result(&[]), LIB), HistoryPage::Empty);
        assert_eq!(classify_page(Some(&history_api_params(10)), &history_api_result(&[]), LIB), HistoryPage::Empty);
    }

    #[test]
    fn filtered_request_is_mutable() {
        let entries = [(8, 990), (9, 995), (10, 1000)];
        let condenser = json!(["alice", 10, 3, 1, 0]);
        let history_api = json!({"account": "alice", "start": 10, "limit": 3, "operation_filter_high": "4"});
        assert_eq!(classify_page(Some(&condenser), &condenser_result(&entries), LIB), HistoryPage::Mutable);
        assert_eq!(classify_page(Some(&history_api), &history_api_result(&entries), LIB), HistoryPage::Mutable);
    }

    #[test]
    fn zero_filter_counts_as_unfiltered() {
        let entries = [(8, 990), (9, 995), (10, 1000)];
        let condenser = json!(["alice", 10, 3, 0, "0"]);
        assert_eq!(classify_page(Some(&condenser), &condenser_result(&entries), LIB), HistoryPage::Fixed);
    }

    #[test]
    fn unrecognized_shapes_are_mutable() {
        let entries = [(10, 1000)];
        assert_eq!(classify_page(None, &condenser_result(&entries), LIB), HistoryPage::Mutable);
        assert_eq!(classify_page(Some(&condenser_params(10)), &json!({"block": 5}), LIB), HistoryPage::Mutable);
        assert_eq!(classify_page(Some(&condenser_params(10)), &json!([[10, {"op": []}]]), LIB), HistoryPage::Mutable);
    }
}
