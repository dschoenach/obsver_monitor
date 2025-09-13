// FileUtils.hpp
#pragma once

#include "DataTypes.hpp"
#include <string>
#include <vector>

FileInfo parse_filename(const std::string& path);

void read_data_file(const std::string& filepath, bool is_vfld, int& version_flag,
                    std::vector<SurfaceStation>& stations, std::vector<TempLevel>& temp_levels);