// use crate::{meta_partition::PartitionFactory, PartitionArgs};
//
// fn t() {
//     // Example columns for the partition
//     let columns = vec![
//         "start_date DATE".to_string(),
//         "end_date DATE".to_string(),
//         // Add more columns as needed
//     ];
//
//     // Create PartitionArgs for a Range partition
//     let partition_args = PartitionArgs {
//         name: "my_partition".to_string(),
//         columns,
//     };
//
//     // Create a PartitionFactory instance for a Range partition
//     let partition_factory = PartitionFactory::Range(partition_args);
// }
//
//
//

// pub fn generate_test_series_(interval: i64, count: i64) -> String {
//     let v = 1..count;
//     let vals = v
//         .into_iter()
//         .map(|c| {
//             format!(
//                 "{} {}, {} {}",
//                 "col1",
//                 1707480000 + c * interval,
//                 "col2",
//                 "random data"
//             )
//         })
//         .collect::<Vec<String>>()
//         .join(",");
//     format!("({})", vals)
// }
//
// pub fn generate_test_series(interval: i64, count: i64) -> String {
//     let v = 0..count;
//     let vals = v
//         .into_iter()
//         .map(|c| {
//             format!(
//                 "({}, {})",
//                 1707480000 + ((c / 2) * interval),
//                 "'random_data'"
//             )
//         })
//         .collect::<Vec<String>>()
//         .join(",");
//     format!("insert nto sad (col, col2) values {}", vals)
// }
//
