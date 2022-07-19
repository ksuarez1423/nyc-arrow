#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>

#include <iostream>

using arrow::Status;

namespace { 

Status RunMain(int argc, char** argv) {
  
  // Working with large data

  arrow::fs::S3GlobalOptions defaults;
  arrow::fs::InitializeS3(defaults);

  std::shared_ptr<arrow::fs::S3FileSystem> fs;
  arrow::fs::S3Options s3conf = arrow::fs::S3Options::Anonymous();
  s3conf.region = "us-east-2";

  ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::S3FileSystem::Make(s3conf));

  arrow::fs::FileSelector selector;
  selector.recursive = true; 
  selector.base_dir = "ursa-labs-taxi-data-v2";
  fs->GetFileInfo(selector);
  ARROW_ASSIGN_OR_RAISE(std::vector<arrow::fs::FileInfo> file_infos,
                      fs->GetFileInfo(selector));
  
  int num_printed = 0;
  int64_t size = 0;
  for (const auto& path : file_infos) {
    if (path.IsFile()) {
      std::cout << path.path() <<  std::endl;
      size += path.size();
    }
  }

  std::cout << "Size in bytes: " << size << std::endl;

  arrow::dataset::FileSystemFactoryOptions options;
  // We'll use Hive-style partitioning. We'll let Arrow Datasets infer the partition
  // schema.
  options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
  auto read_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  auto factory = arrow::dataset::FileSystemDatasetFactory::Make(fs, selector, read_format,
                                                    options)
                     .ValueOrDie();
  auto read_dataset = factory->Finish().ValueOrDie();
  // Print out the fragments
  for (const auto& fragment : read_dataset->GetFragments().ValueOrDie()) {
    std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
  }
  // Read the entire dataset as a Table
  auto read_scan_builder = read_dataset->NewScan().ValueOrDie();
  auto read_scanner = read_scan_builder->Finish().ValueOrDie();
  


  // Working with large data
  std::shared_ptr<arrow::fs::FileSystem> local_fs;
  ARROW_ASSIGN_OR_RAISE(local_fs, arrow::fs::FileSystemFromUriOrPath("/Users/kaesuarez/Data/"));
  const std::string& path = "nyc_taxi";

  ARROW_RETURN_NOT_OK(local_fs->CreateDir(path));
  // The partition schema determines which fields are part of the partitioning.
  auto partition_schema = arrow::schema({arrow::field("year", arrow::utf8()), arrow::field("month", arrow::utf8())});
  // We'll use Hive-style partitioning, which creates directories with "key=value" pairs.
  auto partitioning = std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
  // We'll write Parquet files.
  auto write_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  arrow::dataset::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = write_format->DefaultWriteOptions();
  write_options.filesystem = local_fs;
  write_options.base_dir = path;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.parquet";
  ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options, read_scanner));

 

  return Status::OK();
}
}
int main(int argc, char** argv) {
  Status st = RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
