pub fn page_offset(page: u32, page_size: u32) -> u32 {
    page.saturating_sub(1) * page_size
}

#[cfg(test)]
mod tests {
    use super::page_offset;

    #[test]
    fn calculates_page_offsets() {
        assert_eq!(page_offset(1, 100), 0);
        assert_eq!(page_offset(3, 50), 100);
    }
}
