// verify_cpp_parallel.cpp (Version 13 - Fixed Date-Time Arithmetic and CSV Output)
// - MODIFIED: Fixed date-time arithmetic to avoid DST issues (removed mktime/localtime dependencies).
// - MODIFIED: CSV output now truncates existing files to avoid appending to old data.
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
    double temp = -999.0;   // TT
    double fi   = -999.0;   // FI (geopotential / height)
    double rh   = -999.0;   // RH
    double qq   = -999.0;   // Specific humidity
    double dd   = -999.0;   // Wind direction
    double ff   = -999.0;   // Wind speed
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

// --- Helper Function for Date-Time Arithmetic (REPLACED: remove mktime/localtime DST issues) ---
static inline bool is_leap(int y){
    return ( (y%4==0 && y%100!=0) || (y%400==0) );
}
static inline int days_in_month(int y,int m){
    static const int md[12]={31,28,31,30,31,30,31,31,30,31,30,31};
    if(m==2) return md[1] + (is_leap(y)?1:0);
    return md[m-1];
}
long long add_hours_to_yyyymmddhh(long long start_time, int hours_to_add) {
    int year  = (int)(start_time / 1000000LL);
    int month = (int)((start_time / 10000LL) % 100LL);
    int day   = (int)((start_time / 100LL) % 100LL);
    int hour  = (int)(start_time % 100LL);

    long long total_hours = (long long)hour + hours_to_add;

    while (total_hours >= 24) {
        total_hours -= 24;
        day++;
        int dim = days_in_month(year, month);
        if (day > dim) {
            day = 1;
            month++;
            if (month > 12) { month = 1; year++; }
        }
    }
    while (total_hours < 0) {
        total_hours += 24;
        day--;
        if (day < 1) {
            month--;
            if (month < 1) { month = 12; year--; }
            day = days_in_month(year, month);
        }
    }
    return (long long)year * 1000000LL + (long long)month * 10000LL + (long long)day * 100LL + total_hours;
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
                    if(temp_col_map.count("FI")&&temp_col_map.at("FI")<values.size())tl.fi=values[temp_col_map.at("FI")];
                    if(temp_col_map.count("RH")&&temp_col_map.at("RH")<values.size())tl.rh=values[temp_col_map.at("RH")];
                    if(temp_col_map.count("QQ")&&temp_col_map.at("QQ")<values.size())tl.qq=values[temp_col_map.at("QQ")];
                    if(temp_col_map.count("DD")&&temp_col_map.at("DD")<values.size())tl.dd=values[temp_col_map.at("DD")];
                    if(temp_col_map.count("FF")&&temp_col_map.at("FF")<values.size())tl.ff=values[temp_col_map.at("FF")];
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
    std::string basename = fs::path(path).filename().string();
    FileInfo info; info.path = path;
    // Accept ONLY vfld files ending with 12 digits: YYYYMMDDHHLL (base + 2-digit lead)
    if (basename.rfind("vfld", 0) == 0) {
        std::smatch m;
        std::regex re("(20\\d{6})(\\d{2})(\\d{2})$"); // (YYYYMMDD)(HH)(LL)
        if (std::regex_search(basename, m, re)) {
            info.type = "vfld";
            std::string date_part = m[1].str();
            std::string hour_part = m[2].str();
            std::string lead_part = m[3].str();
            info.base_time = std::stoll(date_part + hour_part); // YYYYMMDDHH
            info.lead_time = std::stoi(lead_part);
            info.valid_time = add_hours_to_yyyymmddhh(info.base_time, info.lead_time);
        }
    } else if (basename.rfind("vobs", 0) == 0) {
        std::smatch m;
        std::regex re("(20\\d{6})(\\d{2})$"); // YYYYMMDDHH
        if (std::regex_search(basename, m, re)) {
            info.type = "vobs";
            info.base_time = std::stoll(m[1].str() + m[2].str());
            info.valid_time = info.base_time;
            info.experiment = "observation";
            info.lead_time = -1;
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

    const std::vector<std::string> supported_variables = {"NN","DD","FF","TT","RH","PS","PE","QQ","VI","TD","TX","TN","GG","GX","FX","FI"};
    
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

                // Optional (small) optimization: build a lookup map for vobs by (station_id, pressure)
                std::unordered_multimap<long long, const TempLevel*> vobs_index;
                vobs_index.reserve(vobs_temp_levels.size()*2);
                auto mk_key = [](int sid, double pres){
                    // Combine into 64-bit key (pressure * 100 to avoid FP noise; assumes pressure < 1,000,000 hPa)
                    long long p = static_cast<long long>(std::llround(pres * 100.0));
                    return ( (static_cast<long long>(sid) << 32) ^ p );
                };
                for (const auto& lvl : vobs_temp_levels) {
                    vobs_index.emplace(mk_key(lvl.station_id, lvl.pressure), &lvl);
                }

                auto process_temp_var = [&](const std::string& var_name, double vfld_val, double vobs_val, double pressure){
                    if (vfld_val > -98.0 && vobs_val > -98.0) {
                        double error = vfld_val - vobs_val;
                        TempResultKey key = {vfld_info.experiment, vfld_info.lead_time, var_name, pressure, vfld_info.valid_time};
                        auto& bucket = local_temp_results[key];
                        bucket.sum_of_errors += error;
                        bucket.sum_of_squared_errors += error * error;
                        bucket.count++;
                    }
                };

                for(const auto& tl_vfld : vfld_temp_levels_vec) {
                    long long key = mk_key(tl_vfld.station_id, tl_vfld.pressure);
                    auto range = vobs_index.equal_range(key);
                    for (auto it = range.first; it != range.second; ++it) {
                        const TempLevel* tl_vobs = it->second;

                        // TT
                        process_temp_var("TT", tl_vfld.temp, tl_vobs->temp, tl_vfld.pressure);
                        // RH
                        process_temp_var("RH", tl_vfld.rh, tl_vobs->rh, tl_vfld.pressure);
                        // FI
                        process_temp_var("FI", tl_vfld.fi, tl_vobs->fi, tl_vfld.pressure);
                        // QQ
                        process_temp_var("QQ", tl_vfld.qq, tl_vobs->qq, tl_vfld.pressure);
                        // DD
                        process_temp_var("DD", tl_vfld.dd, tl_vobs->dd, tl_vfld.pressure);
                        // FF
                        process_temp_var("FF", tl_vfld.ff, tl_vobs->ff, tl_vfld.pressure);

                        break; // Single match per (station,pressure)
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
    
    // Ensure old CSVs removed to avoid stale data
    if (fs::exists("surface_metrics.csv")) fs::remove("surface_metrics.csv");
    if (fs::exists("temp_metrics.csv")) fs::remove("temp_metrics.csv");

    std::cout << "Saving surface metrics to surface_metrics.csv" << std::endl;
    std::ofstream outfile("surface_metrics.csv", std::ios::trunc);
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
    std::ofstream temp_outfile("temp_metrics.csv", std::ios::trunc);
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
