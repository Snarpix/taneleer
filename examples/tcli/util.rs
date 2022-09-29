use comfy_table::*;
use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

pub fn create_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_width(120);
    table
}
