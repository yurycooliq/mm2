use mm2_strategy::{Input, Rules, decide};
use serde_json::{Value, json};

#[test]
fn bounded_rust_typescript_contract_fixtures() {
    let fixture: Vec<Value> = serde_json::from_str(include_str!("parity.json")).unwrap();
    assert!(fixture.len() <= 32);
    for case in fixture {
        let name = case["name"].as_str().unwrap();
        let mut rules = serde_json::to_value(Rules::default()).unwrap();
        for (key, value) in case["rules"].as_object().unwrap() {
            rules[key] = value.clone();
        }
        let rules: Rules = serde_json::from_value(rules).unwrap();
        let mut input = json!({"now_ms":54000000,"market_start_ms":53900000,"market_end_ms":54300000,
            "up":{"bid_micros":600000,"ask_micros":610000,"received_ms":54000000},
            "down":{"bid_micros":390000,"ask_micros":400000,"received_ms":54000000},
            "inventory_up_micros":0,"inventory_down_micros":0,"reserved_up_micros":0,
            "reserved_down_micros":0,"available_cash_micros":1000000000,
            "tick_micros":10000,"min_order_micros":5000000,
            "signals":{"mid_vol_micros":null,"model_up_micros":null,"imbalance_ppm":null,"flow_ppm":null,"received_ms":0}});
        for (key, value) in case["input"].as_object().unwrap() {
            input[key] = value.clone();
        }
        let result = decide(&rules, &serde_json::from_value::<Input>(input).unwrap()).unwrap();
        let actual: Vec<Value> = result
            .quotes
            .iter()
            .map(|q| json!([q.slot, q.outcome, q.price_micros, q.quantity_micros]))
            .collect();
        assert_eq!(json!(actual), case["expected"], "{name}");
        assert_eq!(
            serde_json::to_value(result.reason).unwrap(),
            case["reason"],
            "{name}"
        );
    }
}
