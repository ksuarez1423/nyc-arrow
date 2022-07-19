project(MyExample)
set (CMAKE_CXX_STANDARD 11)
find_package(Arrow REQUIRED) 
get_filename_component(ARROW_CONFIG_PATH ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${ARROW_CONFIG_PATH})
find_package(ArrowDataset REQUIRED HINTS ${ARROW_CONFIG_PATH})

add_executable(download download.cc)
target_link_libraries(download PRIVATE arrow_shared parquet arrow_dataset_shared)
