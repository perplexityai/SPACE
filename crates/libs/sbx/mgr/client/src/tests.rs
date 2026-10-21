use super::*;

#[test]
fn list_paging_ends_on_an_empty_cursor_and_rejects_a_stalled_one() {
    assert_eq!(next_list_page_token("", String::new()).unwrap(), None);
    assert_eq!(
        next_list_page_token("", "a".to_owned()).unwrap(),
        Some("a".to_owned())
    );
    assert_eq!(
        next_list_page_token("a", "b".to_owned()).unwrap(),
        Some("b".to_owned())
    );
    assert_eq!(
        next_list_page_token("a", "a".to_owned())
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
}
