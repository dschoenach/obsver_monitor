// verify_cpp_parallel.cpp (Version 12 - Added VT Hour for Scorecard Compatibility)
// - ADDED: ResultKey and TempResultKey now include vt_hour for compatibility with the scorecard tool.
// - MODIFIED: Output CSV files surface_metrics.csv and temp_metrics.csv now include vt_hour column.
// - No other logical changes were made.

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <regex>
#include <map>
#include <unordered_map>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <omp.h>
#include <ctime> // Added for correct date-time calculations

namespace fs = std::filesystem;

// --- Data Structures ---
struct SurfaceStation {
    int id = -1;
    double lat = -999.0, lon = -999.0, hgt = -999.0;
    double nn = -999.0, dd = -999.0, ff = -999.0, tt = -999.0, rh = -999.0, ps = -999.0;
    double pe = -999.0, qq = -999.0, vi = -999.0, td = -999.0, tx = -999.0, tn = -999.0;
    double gg = -999.0, gx = -999.0, fx = -999.0;
};
struct TempLevel {
    int station_id = -1;
    double pressure = -999.0;
    double temp = -999.0;
    double rh = -999.0;
};
struct VobsData {
    std::unordered_map<int, SurfaceStation> stations;
    std::vector<TempLevel> temp_levels;
};
struct FileInfo {
    std::string path, type, experiment;
    long long base_time = 0, valid_time = 0;
    int lead_time = -1;
};
struct ResultKey {
    std::string experiment;
    int lead_time;
    std::string variable;
    long long vt_hour; // Changed to long long for full timestamp
    bool operator<(const ResultKey& other) const {
        if (experiment != other.experiment) return experiment < other.experiment;
        if (lead_time != other.lead_time) return lead_time < other.lead_time;
        if (vt_hour != other.vt_hour) return vt_hour < other.vt_hour;
        return variable < other.variable;
    }
};
struct TempResultKey {
    std::string experiment;
    int lead_time;
    std::string variable;
    double pressure_level;
    long long vt_hour; // Changed to long long for full timestamp
    bool operator<(const TempResultKey& other) const {
        if (experiment != other.experiment) return experiment < other.experiment;
        if (lead_time != other.lead_time) return lead_time < other.lead_time;
        if (vt_hour != other.vt_hour) return vt_hour < other.vt_hour;
        if (pressure_level != other.pressure_level) return pressure_level < other.pressure_level;
        return variable < other.variable;
    }
};
struct AggregatedStats {
    double sum_of_errors = 0.0, sum_of_squared_errors = 0.0;
    long count = 0;
};

// --- Helper Function for Date-Time Arithmetic ---
long long add_hours_to_yyyymmddhh(long long start_time, int hours_to_add) {
    long long year = start_time / 1000000;
    long long month = (start_time / 10000) % 100;
    long long day = (start_time / 100) % 100;
    long long hour = start_time % 100;

    std::tm time_info = {};
    time_info.tm_year = year - 1900;
    time_info.tm_mon = month - 1;
    time_info.tm_mday = day;
    time_info.tm_hour = hour + hours_to_add; // Add lead time

    // mktime normalizes the date and time, handling day/month/year rollovers
    std::time_t time_normalized = std::mktime(&time_info);
    
    // Convert back to the YYYYMMDDHH format
    std::tm* final_time_info = std::localtime(&time_normalized);
    long long final_time = (final_time_info->tm_year + 1900LL) * 1000000LL +
                           (final_time_info->tm_mon + 1LL) * 10000LL +
                           (final_time_info->tm_mday) * 100LL +
                           (final_time_info->tm_hour);
    return final_time;
}

// --- Core Functions ---
void read_data_file(const std::string& filepath, bool is_vfld, int& version_flag, std::vector<SurfaceStation>& stations, std::vector<TempLevel>& temp_levels) {
    std::ifstream file(filepath);
    if (!file.is_open()) { return; }
    stations.clear(); temp_levels.clear();
    try {
        std::string line;
        int num_stat, num_temp;
        if(!std::getline(file, line)) return;
        std::stringstream(line) >> num_stat >> num_temp >> version_flag;

        std::vector<std::string> surface_variables;
        if (version_flag <= 3) {
            if(!std::getline(file, line)) return;
            surface_variables = {"NN","DD","FF","TT","RH","PS","PE","QQ","VI","TD","TX","TN","GG","GX","FX"};
        } else if (version_flag == 4 || version_flag == 5) {
            if(!std::getline(file, line)) return; int ninvar = std::stoi(line);
            for (int i=0; i<ninvar; ++i) { if(!std::getline(file, line)) break; std::stringstream ss(line); std::string vn; ss >> vn; surface_variables.push_back(vn); }
        }
        std::unordered_map<std::string, int> surface_col_map;
        for (size_t i=0; i<surface_variables.size(); ++i) surface_col_map[surface_variables[i]] = i;
        
        stations.reserve(num_stat);
        for (int i=0; i<num_stat; ++i) {
            if (!std::getline(file, line) || line.empty()) break;
            std::stringstream line_stream(line); SurfaceStation s={}; line_stream >> s.id >> s.lat >> s.lon;
            if (!is_vfld) { line_stream >> s.hgt; }
            std::vector<double> data_values; double val; while(line_stream >> val) data_values.push_back(val);
            if (is_vfld) {
                if (surface_col_map.count("FI") && surface_col_map.at("FI")<data_values.size()) s.hgt=data_values[surface_col_map.at("FI")];
                else if (surface_col_map.count("hgt") && surface_col_map.at("hgt")<data_values.size()) s.hgt=data_values[surface_col_map.at("hgt")];
            }
            if(surface_col_map.count("NN")&&surface_col_map.at("NN")<data_values.size())s.nn=data_values[surface_col_map.at("NN")];if(surface_col_map.count("DD")&&surface_col_map.at("DD")<data_values.size())s.dd=data_values[surface_col_map.at("DD")];
            if(surface_col_map.count("FF")&&surface_col_map.at("FF")<data_values.size())s.ff=data_values[surface_col_map.at("FF")];if(surface_col_map.count("TT")&&surface_col_map.at("TT")<data_values.size())s.tt=data_values[surface_col_map.at("TT")];
            if(surface_col_map.count("RH")&&surface_col_map.at("RH")<data_values.size())s.rh=data_values[surface_col_map.at("RH")];if(surface_col_map.count("PS")&&surface_col_map.at("PS")<data_values.size())s.ps=data_values[surface_col_map.at("PS")];
            if(surface_col_map.count("PE")&&surface_col_map.at("PE")<data_values.size())s.pe=data_values[surface_col_map.at("PE")];if(surface_col_map.count("QQ")&&surface_col_map.at("QQ")<data_values.size())s.qq=data_values[surface_col_map.at("QQ")];
            if(surface_col_map.count("VI")&&surface_col_map.at("VI")<data_values.size())s.vi=data_values[surface_col_map.at("VI")];if(surface_col_map.count("TD")&&surface_col_map.at("TD")<data_values.size())s.td=data_values[surface_col_map.at("TD")];
            if(surface_col_map.count("TX")&&surface_col_map.at("TX")<data_values.size())s.tx=data_values[surface_col_map.at("TX")];if(surface_col_map.count("TN")&&surface_col_map.at("TN")<data_values.size())s.tn=data_values[surface_col_map.at("TN")];
            if(surface_col_map.count("GG")&&surface_col_map.at("GG")<data_values.size())s.gg=data_values[surface_col_map.at("GG")];if(surface_col_map.count("GX")&&surface_col_map.at("GX")<data_values.size())s.gx=data_values[surface_col_map.at("GX")];
            if(surface_col_map.count("FX")&&surface_col_map.at("FX")<data_values.size())s.fx=data_values[surface_col_map.at("FX")];
            stations.push_back(s);
        }

        if (num_temp > 0) {
            if(!std::getline(file, line) || line.empty()) return; int num_temp_lev = std::stoi(line);
            if(!std::getline(file, line) || line.empty()) return; int ninvar_temp = std::stoi(line);
            std::vector<std::string> temp_variables;
            for(int i=0; i<ninvar_temp; ++i) { if(!std::getline(file, line)) break; std::stringstream ss(line); std::string vn; ss >> vn; temp_variables.push_back(vn); }
            std::unordered_map<std::string, int> temp_col_map;
            for(size_t i=0; i<temp_variables.size(); ++i) temp_col_map[temp_variables[i]] = i;
            temp_levels.reserve(num_temp * num_temp_lev);
            for(int i=0; i<num_temp; ++i) {
                if (!std::getline(file, line) || line.empty()) break;
                std::stringstream header_ss(line); int station_id; header_ss >> station_id;
                for(int j=0; j<num_temp_lev; ++j) {
                    if (!std::getline(file, line)) break;
                    std::stringstream level_ss(line); std::vector<double> values; double val; while(level_ss >> val) values.push_back(val);
                    if (values.empty()) continue;
                    TempLevel tl; tl.station_id = station_id;
                    if(temp_col_map.count("PP")&&temp_col_map.at("PP")<values.size())tl.pressure=values[temp_col_map.at("PP")];
                    if(temp_col_map.count("TT")&&temp_col_map.at("TT")<values.size())tl.temp=values[temp_col_map.at("TT")];
                    if(temp_col_map.count("RH")&&temp_col_map.at("RH")<values.size())tl.rh=values[temp_col_map.at("RH")];
                    temp_levels.push_back(tl);
                }
            }
        }
    } catch (const std::exception& e) {
        #pragma omp critical
        std::cerr << "Warning: Exception caught while reading " << filepath << ": " << e.what() << ". Skipping file." << std::endl;
    }
}

FileInfo parse_filename(const std::string& path) {
    std::string basename = fs::path(path).filename().string(); FileInfo info; info.path = path;
    if (basename.rfind("vfld", 0) == 0) {
        std::regex rgx("vfld.*?(20\\d{8})(\\d{2})?$"); std::smatch match;
        if (std::regex_search(basename, match, rgx)) {
            info.type = "vfld"; info.base_time = std::stoll(match[1].str());
            if (match.size() > 2 && match[2].matched) {
                info.lead_time = std::stoi(match[2].str());
                // Use the helper function for correct date-time addition
                info.valid_time = add_hours_to_yyyymmddhh(info.base_time, info.lead_time);
            } else {
                info.lead_time = -1;
                info.valid_time = info.base_time;
            }
        }
    } else if (basename.rfind("vobs", 0) == 0) {
        std::regex rgx("vobs(20\\d{8})$"); std::smatch match;
        if (std::regex_search(basename, match, rgx)) {
            info.type = "vobs"; info.valid_time = std::stoll(match[1].str());
            info.base_time = info.valid_time; info.experiment = "observation";
        }
    }
    return info;
}


int main(int argc, char* argv[]) {
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <start_YYYYMMDDHH> <end_YYYYMMDDHH> <fcint> <vobs_dir> <vfld_exp_dir1> [<vfld_exp_dir2> ...]" << std::endl; return 1;
    }
    long long start_dt = std::stoll(argv[1]);
    long long end_dt = std::stoll(argv[2]);
    int fcint;
    try {
        fcint = std::stoi(argv[3]);
    } catch (const std::exception& e) {
        std::cerr << "Error: Invalid fcint '" << argv[3] << "'. Must be an integer." << std::endl; return 1;
    }
    const fs::path vobs_path = argv[4];
    std::vector<fs::path> experiment_paths;
    for (int i = 5; i < argc; ++i) { experiment_paths.push_back(argv[i]); }

    auto script_start_time = std::chrono::high_resolution_clock::now();
    
    std::vector<FileInfo> vfld_files;
    std::vector<FileInfo> vobs_files;
    std::cout << "Discovering and parsing filenames..." << std::endl;
    std::map<std::string, int> max_lead_time_found; // To store max lead time per experiment

    for (const auto& exp_path_raw : experiment_paths) {
        fs::path exp_path = exp_path_raw;
        std::string experiment_name = exp_path.filename().string();
        if (experiment_name.empty() || experiment_name == ".") { experiment_name = exp_path.parent_path().filename().string(); }
        max_lead_time_found[experiment_name] = 0; // Initialize max lead time

        std::cout << "Searching for vfld files for experiment '" << experiment_name << "' in: " << exp_path << std::endl;
        for (const auto& entry : fs::directory_iterator(exp_path)) {
            if (entry.is_regular_file()) {
                FileInfo info = parse_filename(entry.path().string());
                if (info.type == "vfld" && info.base_time >= start_dt && info.base_time <= end_dt) {
                    long long base_hour = info.base_time % 100;
                    if (base_hour % fcint != 0) { continue; }
                    info.experiment = experiment_name;
                    vfld_files.push_back(info);
                    if (info.lead_time > max_lead_time_found[experiment_name]) {
                        max_lead_time_found[experiment_name] = info.lead_time;
                    }
                }
            }
        }
    }
    std::cout << "Searching for vobs files in: " << vobs_path << std::endl;
    for (const auto& entry : fs::recursive_directory_iterator(vobs_path)) {
        if (entry.is_regular_file()) { FileInfo info = parse_filename(entry.path().string()); if (info.type == "vobs") vobs_files.push_back(info); }
    }
    std::cout << "Found " << vobs_files.size() << " vobs files and " << vfld_files.size() << " vfld files to process (after fcint filter)." << std::endl;
    for(const auto& pair : max_lead_time_found) {
        std::cout << "  - Max lead time found for " << pair.first << ": +" << pair.second << "h" << std::endl;
    }
    if (vfld_files.empty() || vobs_files.empty()) { std::cerr << "Error: No data files found. Please check paths and date range." << std::endl; return 1; }

    auto vobs_read_start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Reading all vobs files into memory (in parallel)..." << std::endl;
    std::unordered_map<long long, VobsData> vobs_data_map;
    #pragma omp parallel for
    for (size_t i = 0; i < vobs_files.size(); ++i) {
        const auto& vobs_info = vobs_files[i];
        int version;
        std::vector<SurfaceStation> stations_vec;
        std::vector<TempLevel> temp_levels_vec;
        read_data_file(vobs_info.path, false, version, stations_vec, temp_levels_vec);
        #pragma omp critical
        {
            for (const auto& station : stations_vec) { vobs_data_map[vobs_info.valid_time].stations[station.id] = station; }
            vobs_data_map[vobs_info.valid_time].temp_levels.insert(vobs_data_map[vobs_info.valid_time].temp_levels.end(), temp_levels_vec.begin(), temp_levels_vec.end());
        }
    }
    auto vobs_read_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> vobs_read_duration = vobs_read_end_time - vobs_read_start_time;
    std::cout << "--- Time to read all vobs files: " << vobs_read_duration.count() << " seconds ---" << std::endl;

    auto verification_start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Starting main verification loop (in parallel)..." << std::endl;
    std::map<ResultKey, AggregatedStats> final_surface_results;
    std::map<TempResultKey, AggregatedStats> final_temp_results;

    const std::vector<std::string> supported_variables = {"NN","DD","FF","TT","RH","PS","PE","QQ","VI","TD","TX","TN","GG","GX","FX"};

    #pragma omp parallel
    {
        std::map<ResultKey, AggregatedStats> local_surface_results;
        std::map<TempResultKey, AggregatedStats> local_temp_results;
        #pragma omp for nowait
        for (size_t i = 0; i < vfld_files.size(); ++i) {
            const auto& vfld_info = vfld_files[i];
            auto it_vobs = vobs_data_map.find(vfld_info.valid_time);
            if (it_vobs == vobs_data_map.end()) { continue; }
            const auto& vobs_stations = it_vobs->second.stations;
            const auto& vobs_temp_levels = it_vobs->second.temp_levels;
            int version;
            std::vector<SurfaceStation> vfld_stations_vec;
            std::vector<TempLevel> vfld_temp_levels_vec;
            read_data_file(vfld_info.path, true, version, vfld_stations_vec, vfld_temp_levels_vec);
            for (const auto& station_vfld : vfld_stations_vec) {
                auto it_station_vobs = vobs_stations.find(station_vfld.id);
                if (it_station_vobs != vobs_stations.end()) {
                    const auto& station_vobs = it_station_vobs->second;
                    auto process_var = [&](const std::string& var, double vfld_val, double vobs_val){
                        if (vfld_val > -98.0 && vobs_val > -98.0) {
                            double error = vfld_val - vobs_val;
                            long long vt_hour = vfld_info.valid_time; // Use full valid time
                            ResultKey key = {vfld_info.experiment, vfld_info.lead_time, var, vt_hour};
                            local_surface_results[key].sum_of_errors += error;
                            local_surface_results[key].sum_of_squared_errors += error*error;
                            local_surface_results[key].count++;
                        }
                    };
                    for (const auto& var : supported_variables) {
                        if(var=="NN")process_var("NN",station_vfld.nn,station_vobs.nn);else if(var=="DD")process_var("DD",station_vfld.dd,station_vobs.dd);
                        else if(var=="FF")process_var("FF",station_vfld.ff,station_vobs.ff);else if(var=="TT")process_var("TT",station_vfld.tt,station_vobs.tt);
                        else if(var=="RH")process_var("RH",station_vfld.rh,station_vobs.rh);else if(var=="PS")process_var("PS",station_vfld.ps,station_vobs.ps);
                        else if(var=="PE")process_var("PE",station_vfld.pe,station_vobs.pe);else if(var=="QQ")process_var("QQ",station_vfld.qq,station_vobs.qq);
                        else if(var=="VI")process_var("VI",station_vfld.vi,station_vobs.vi);else if(var=="TD")process_var("TD",station_vfld.td,station_vobs.td);
                        else if(var=="TX")process_var("TX",station_vfld.tx,station_vobs.tx);else if(var=="TN")process_var("TN",station_vfld.tn,station_vobs.tn);
                        else if(var=="GG")process_var("GG",station_vfld.gg,station_vobs.gg);else if(var=="GX")process_var("GX",station_vfld.gx,station_vobs.gx);
                        else if(var=="FX")process_var("FX",station_vfld.fx,station_vobs.fx);
                    }
                }
            }
            if (!vfld_temp_levels_vec.empty() && !vobs_temp_levels.empty()) {
                for(const auto& tl_vfld : vfld_temp_levels_vec) {
                    for(const auto& tl_vobs : vobs_temp_levels) {
                        if (tl_vfld.station_id == tl_vobs.station_id && std::abs(tl_vfld.pressure - tl_vobs.pressure) < 1e-5) {
                            long long vt_hour = vfld_info.valid_time; // Use full valid time
                            if (tl_vfld.temp > -98.0 && tl_vobs.temp > -98.0) {
                                double error = tl_vfld.temp - tl_vobs.temp;
                                TempResultKey key = {vfld_info.experiment, vfld_info.lead_time, "TT", tl_vfld.pressure, vt_hour};
                                local_temp_results[key].sum_of_errors += error; local_temp_results[key].sum_of_squared_errors += error*error; local_temp_results[key].count++;
                            }
                            if (tl_vfld.rh > -98.0 && tl_vobs.rh > -98.0) {
                                double error = tl_vfld.rh - tl_vobs.rh;
                                TempResultKey key = {vfld_info.experiment, vfld_info.lead_time, "RH", tl_vfld.pressure, vt_hour};
                                local_temp_results[key].sum_of_errors += error; local_temp_results[key].sum_of_squared_errors += error*error; local_temp_results[key].count++;
                            }
                            break;
                        }
                    }
                }
            }
        }
        #pragma omp critical
        {
            for(const auto&p:local_surface_results){final_surface_results[p.first].sum_of_errors+=p.second.sum_of_errors;final_surface_results[p.first].sum_of_squared_errors+=p.second.sum_of_squared_errors;final_surface_results[p.first].count+=p.second.count;}
            for(const auto&p:local_temp_results){final_temp_results[p.first].sum_of_errors+=p.second.sum_of_errors;final_temp_results[p.first].sum_of_squared_errors+=p.second.sum_of_squared_errors;final_temp_results[p.first].count+=p.second.count;}
        }
    }
    auto verification_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> verification_duration = verification_end_time - verification_start_time;
    std::cout << "--- Time for verification processing: " << verification_duration.count() << " seconds ---" << std::endl;
    
    std::cout << "Saving surface metrics to surface_metrics.csv" << std::endl;
    std::ofstream outfile("surface_metrics.csv");
    outfile.precision(6);
    outfile << std::fixed << "experiment,lead_time,vt_hour,obstypevar,bias,rmse,n_samples\n";
    for(const auto& pair : final_surface_results) {
        const auto& key = pair.first; const auto& stats = pair.second;
        if (stats.count > 0) {
            double bias = stats.sum_of_errors / stats.count; double rmse = std::sqrt(stats.sum_of_squared_errors / stats.count);
            outfile << key.experiment << "," << key.lead_time << "," << key.vt_hour << "," << key.variable << "," << bias << "," << rmse << "," << stats.count << "\n";
        }
    }
    outfile.close();

    std::cout << "Saving temp metrics to temp_metrics.csv" << std::endl;
    std::ofstream temp_outfile("temp_metrics.csv");
    temp_outfile.precision(6);
    temp_outfile << std::fixed << "experiment,lead_time,vt_hour,pressure_level,obstypevar,bias,rmse,n_samples\n";
    for(const auto& pair : final_temp_results) {
        const auto& key = pair.first; const auto& stats = pair.second;
        if (stats.count > 0) {
            double bias = stats.sum_of_errors / stats.count; double rmse = std::sqrt(stats.sum_of_squared_errors / stats.count);
            temp_outfile << key.experiment << "," << key.lead_time << "," << key.vt_hour << "," << key.pressure_level << "," << key.variable
                         << "," << bias << "," << rmse << "," << stats.count << "\n";
        }
    }
    temp_outfile.close();

    auto script_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> script_duration = script_end_time - script_start_time;
    std::cout << "\n--- Total script execution time: " << script_duration.count() << " seconds ---" << std::endl;

    return 0;
}
