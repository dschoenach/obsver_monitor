// verify_cpp_parallel.cpp
// Note: Restricts processing to common valid_time across all experiments
// (no station/level key intersection; dates-only for speed and simplicity)
#include "DataTypes.hpp"
#include "FileUtils.hpp"
#include "VerificationUtils.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <omp.h>
#include <cstdlib>
#include <sstream>

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    // Simplified argument parsing
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <start_YYYYMMDDHH> <end_YYYYMMDDHH> <fcint> <vobs_dir> <vfld_exp_dir1> [<vfld_exp_dir2> ...]" << std::endl;
        return 1;
    }

    long long start_dt = std::stoll(argv[1]);
    long long end_dt = std::stoll(argv[2]);
    int fcint;
    try {
        fcint = std::stoi(argv[3]);
    } catch (const std::exception& e) {
        std::cerr << "Error: Invalid fcint '" << argv[3] << "'. Must be an integer." << std::endl;
        return 1;
    }
    const fs::path vobs_path = argv[4];
    
    std::vector<fs::path> experiment_paths;
    for (int i = 5; i < argc; ++i) {
        experiment_paths.push_back(argv[i]);
    }

    auto script_start_time = std::chrono::high_resolution_clock::now();
    
    std::vector<FileInfo> vfld_files;
    std::vector<FileInfo> vobs_files;
    // Track valid_time availability per experiment and for observations
    std::unordered_map<std::string, std::unordered_set<long long>> exp_valid_times;
    std::unordered_set<long long> vobs_valid_times;
    std::cout << "Discovering and parsing filenames..." << std::endl;
    for (const auto& exp_path_raw : experiment_paths) {
        fs::path exp_path = exp_path_raw;
        std::string experiment_name = exp_path.filename().string();
        if (experiment_name.empty() || experiment_name == ".") { experiment_name = exp_path.parent_path().filename().string(); }
        for (const auto& entry : fs::directory_iterator(exp_path)) {
            if (entry.is_regular_file()) {
                FileInfo info = parse_filename(entry.path().string());
                if (info.type == "vfld" && info.base_time >= start_dt && info.base_time <= end_dt && (info.base_time % 100) % fcint == 0) {
                    info.experiment = experiment_name;
                    vfld_files.push_back(info);
                    exp_valid_times[experiment_name].insert(info.valid_time);
                }
            }
        }
    }
    for (const auto& entry : fs::recursive_directory_iterator(vobs_path)) {
        if (!entry.is_regular_file()) continue;
        FileInfo info = parse_filename(entry.path().string());
        if (info.type != "vobs") continue;
        // Filter vobs by start/end like vfld (using valid_time)
        if (info.valid_time >= start_dt && info.valid_time <= end_dt) {
            vobs_files.push_back(info);
            vobs_valid_times.insert(info.valid_time);
        }
    }
    std::cout << "Found " << vobs_files.size() << " vobs files and " << vfld_files.size() << " vfld files (pre-filter)." << std::endl;

    // Compute intersection of valid_time across all experiments, then intersect with available vobs times
    std::unordered_set<long long> common_valid_times;
    bool first_exp = true;
    for (const auto& kv : exp_valid_times) {
        const auto& times = kv.second;
        if (first_exp) {
            common_valid_times = times;
            first_exp = false;
        } else {
            std::unordered_set<long long> tmp;
            for (auto t : common_valid_times) if (times.find(t) != times.end()) tmp.insert(t);
            common_valid_times.swap(tmp);
        }
    }
    if (!first_exp) { // if we had at least one experiment, also require vobs availability
        std::unordered_set<long long> tmp;
        for (auto t : common_valid_times) if (vobs_valid_times.find(t) != vobs_valid_times.end()) tmp.insert(t);
        common_valid_times.swap(tmp);
    }

    std::cout << "Experiments: " << exp_valid_times.size() << ", common valid times with vobs: " << common_valid_times.size() << std::endl;
    if (common_valid_times.empty()) {
        std::cerr << "Error: No common valid times across experiments (and vobs) within given range." << std::endl;
        return 1;
    }
    if (vfld_files.empty() || vobs_files.empty()) { std::cerr << "Error: No data files found." << std::endl; return 1; }

    // Helper to parse space-delimited env lists
    auto parse_env_list = [](const char* name) {
        std::vector<std::string> out;
        const char* env = std::getenv(name);
        if (!env) return out;
        std::string env_str(env);
        std::istringstream iss{env_str};
        std::string tok;
        while (iss >> tok) out.push_back(tok);
        return out;
    };

    // Surface variables to verify (order defines output emphasis)
    std::vector<std::string> supported_variables = {
        // Requested SURFPAR order
        "PS","SPS","FF","GX","DD","TT","TTHA","TN","TX","TD","TDD","RH","QQ","NN","LC","CH","VI"
    };
    {
        auto from_env = parse_env_list("SURFPAR_MONITOR");
        if (!from_env.empty()) supported_variables = std::move(from_env);
    }
    // Temp variables order (upper-air)
    std::vector<std::string> temp_supported_variables = {"TT","TD","FF","DD","FI","RH","QQ"};
    {
        auto from_env = parse_env_list("TEMPPAR_MONITOR");
        if (!from_env.empty()) temp_supported_variables = std::move(from_env);
    }
    // Build precipitation windows from env selection (SURFPAR_MONITOR); fallback to defaults if not provided
    auto build_precip_windows = [&](const std::vector<std::string>& env_list){
        std::vector<std::pair<std::string,int>> out;
        const std::vector<std::pair<std::string,int>> candidates = {
            {"PE1",1},{"PE3",3},{"PE6",6},{"PE12",12},{"PE24",24}
        };
        if (!env_list.empty()) {
            std::unordered_set<std::string> envset(env_list.begin(), env_list.end());
            for (const auto& c : candidates) {
                if (envset.count(c.first)) out.push_back(c);
            }
            return out;
        }
        return candidates;
    };
    const auto precip_windows = build_precip_windows(parse_env_list("SURFPAR_MONITOR"));

    // Precompute forecast cumulative precipitation totals per (experiment|base_time)->lead->station
    std::unordered_map<std::string, std::map<int, std::unordered_map<int,double>>> precip_totals;
    if (!precip_windows.empty()) {
        std::cout << "Precomputing forecast cumulative precipitation totals..." << std::endl;
        for (const auto& fi : vfld_files) {
            int version_tmp;
            std::vector<SurfaceStation> st_tmp;
            std::vector<TempLevel> tl_tmp;
            read_data_file(fi.path, true, version_tmp, st_tmp, tl_tmp);
            std::string key = fi.experiment + "|" + std::to_string(fi.base_time);
            auto& lead_map = precip_totals[key];
            auto& station_map = lead_map[fi.lead_time];
            for (const auto& s : st_tmp) {
                if (s.pe > -98.0) station_map[s.id] = s.pe; // cumulative since start
            }
        }
    } else {
        std::cout << "Skipping precipitation accumulation (no PE windows selected)." << std::endl;
    }

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
    std::cout << "--- Time to read all vobs files: " << std::chrono::duration<double>(vobs_read_end_time - vobs_read_start_time).count() << " seconds ---" << std::endl;

    auto verification_start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Starting verification loop (in parallel)..." << std::endl;
    std::map<ResultKey, AggregatedStats> final_surface_results;
    std::map<TempResultKey, AggregatedStats> final_temp_results;

    
    #pragma omp parallel
    {
        std::map<ResultKey, AggregatedStats> local_surface_results;
        std::map<TempResultKey, AggregatedStats> local_temp_results;
        #pragma omp for nowait
        for (size_t i = 0; i < vfld_files.size(); ++i) {
            const auto& vfld_info = vfld_files[i];
            // Enforce common valid_time across all experiments and vobs
            if (common_valid_times.find(vfld_info.valid_time) == common_valid_times.end()) { continue; }
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
                            double error = (var == "DD") ? directional_diff(vfld_val, vobs_val) : (vfld_val - vobs_val);
                            if (is_missing(error)) return;
                            ResultKey key = {vfld_info.experiment, vfld_info.lead_time, var, vfld_info.valid_time};
                            auto& stats = local_surface_results[key];
                            stats.sum_of_errors += error;
                            stats.sum_of_squared_errors += error*error;
                            stats.count++;
                        }
                    };
                    for (const auto& var : supported_variables) {
                        if(var=="PS")process_var("PS",station_vfld.ps,station_vobs.ps);
                        else if(var=="SPS")process_var("SPS",station_vfld.pss,station_vobs.pss);
                        else if(var=="FF")process_var("FF",station_vfld.ff,station_vobs.ff);
                        else if(var=="GX")process_var("GX",station_vfld.gx,station_vobs.gx);
                        else if(var=="DD")process_var("DD",station_vfld.dd,station_vobs.dd);
                        else if(var=="TT")process_var("TT",station_vfld.tt,station_vobs.tt);
                        else if(var=="TTHA")process_var("TTHA",station_vfld.ttha,station_vobs.ttha);
                        else if(var=="TN")process_var("TN",station_vfld.tn,station_vobs.tn);
                        else if(var=="TX")process_var("TX",station_vfld.tx,station_vobs.tx);
                        else if(var=="TD")process_var("TD",station_vfld.td,station_vobs.td);
                        else if(var=="TDD"){
                            double f = (station_vfld.tt>-98.0 && station_vfld.td>-98.0)? (station_vfld.tt - station_vfld.td) : -999.0;
                            double o = (station_vobs.tt>-98.0 && station_vobs.td>-98.0)? (station_vobs.tt - station_vobs.td) : -999.0;
                            process_var("TDD", f, o);
                        }
                        else if(var=="RH")process_var("RH",station_vfld.rh,station_vobs.rh);
                        else if(var=="QQ")process_var("QQ",station_vfld.qq,station_vobs.qq);
                        else if(var=="NN")process_var("NN",station_vfld.nn,station_vobs.nn);
                        else if(var=="LC")process_var("LC",station_vfld.lc,station_vobs.lc);
                        else if(var=="CH")process_var("CH",station_vfld.ch,station_vobs.ch);
                        else if(var=="VI")process_var("VI",station_vfld.vi,station_vobs.vi);
                    }

                    // Precipitation windows (derive increments from cumulative PE)
                    std::string precip_key = vfld_info.experiment + "|" + std::to_string(vfld_info.base_time);
                    auto pt_it = precip_totals.find(precip_key);
                    if (pt_it != precip_totals.end()) {
                        auto& lead_map = pt_it->second;
                        for (const auto& pw : precip_windows) {
                            const std::string& pvar = pw.first;
                            int win = pw.second;
                            if (vfld_info.lead_time < win) continue; // cannot form window
                            auto it_curr = lead_map.find(vfld_info.lead_time);
                            auto it_prev = lead_map.find(vfld_info.lead_time - win);
                            if (it_curr == lead_map.end() || it_prev == lead_map.end()) continue;
                            auto it_curr_st = it_curr->second.find(station_vfld.id);
                            auto it_prev_st = it_prev->second.find(station_vfld.id);
                            if (it_curr_st == it_curr->second.end() || it_prev_st == it_prev->second.end()) continue;
                            double inc = it_curr_st->second - it_prev_st->second;
                            if (inc < -98.0) continue;
                            double obs_val = get_surface_value(station_vobs, pvar);
                            if (inc > -98.0 && obs_val > -98.0) {
                                double error = inc - obs_val;
                                if (!is_missing(error)) {
                                    ResultKey key = {vfld_info.experiment, vfld_info.lead_time, pvar, vfld_info.valid_time};
                                    auto& stats = local_surface_results[key];
                                    stats.sum_of_errors += error;
                                    stats.sum_of_squared_errors += error*error;
                                    stats.count++;
                                }
                            }
                        }
                    }
                }
            }

            if (!vfld_temp_levels_vec.empty() && !vobs_temp_levels.empty()) {
                std::unordered_multimap<long long, const TempLevel*> vobs_index;
                auto mk_key = [](int sid, double pres){ return ((long long)sid << 32) ^ (long long)std::llround(pres * 100.0); };
                for (const auto& lvl : vobs_temp_levels) vobs_index.emplace(mk_key(lvl.station_id, lvl.pressure), &lvl);

                auto process_temp_var = [&](const std::string& var, const TempLevel& tl_f, const TempLevel& tl_o){
                    double fval = get_temp_value(tl_f, var);
                    double oval = get_temp_value(tl_o, var);
                    if (fval > -98.0 && oval > -98.0) {
                        double error = (var == "DD") ? directional_diff(fval, oval) : (fval - oval);
                        if (is_missing(error)) return;
                        TempResultKey key = {vfld_info.experiment, vfld_info.lead_time, var, tl_f.pressure, vfld_info.valid_time};
                        auto& stats = local_temp_results[key];
                        stats.sum_of_errors += error;
                        stats.sum_of_squared_errors += error * error;
                        stats.count++;
                    }
                };

                for(const auto& tl_vfld : vfld_temp_levels_vec) {
                    auto range = vobs_index.equal_range(mk_key(tl_vfld.station_id, tl_vfld.pressure));
                    if (range.first != range.second) {
                        const TempLevel* tl_vobs = range.first->second;
                        for (const auto& tvar : temp_supported_variables) {
                            process_temp_var(tvar, tl_vfld, *tl_vobs);
                        }
                    }
                }
            }
        }
        #pragma omp critical
        {
            for(const auto& p : local_surface_results) {
                auto& g = final_surface_results[p.first];
                g.sum_of_errors += p.second.sum_of_errors;
                g.sum_of_squared_errors += p.second.sum_of_squared_errors;
                g.count += p.second.count;
            }
            for(const auto& p : local_temp_results) {
                auto& g = final_temp_results[p.first];
                g.sum_of_errors += p.second.sum_of_errors;
                g.sum_of_squared_errors += p.second.sum_of_squared_errors;
                g.count += p.second.count;
            }
        }
    }
    auto verification_end_time = std::chrono::high_resolution_clock::now();
    std::cout << "--- Time for verification processing: " << std::chrono::duration<double>(verification_end_time - verification_start_time).count() << " seconds ---" << std::endl;
    
    if (fs::exists("surface_metrics.csv")) fs::remove("surface_metrics.csv");
    if (fs::exists("temp_metrics.csv")) fs::remove("temp_metrics.csv");

    std::cout << "Saving surface metrics to surface_metrics.csv" << std::endl;
    std::ofstream outfile("surface_metrics.csv", std::ios::trunc);
    outfile.precision(6);
    outfile << std::fixed << "experiment,lead_time,vt_hour,obstypevar,bias,rmse,n_samples\n";
    for(const auto& pair : final_surface_results) {
        if (pair.second.count > 0) {
            double bias = pair.second.sum_of_errors / pair.second.count;
            double rmse = std::sqrt(pair.second.sum_of_squared_errors / pair.second.count);
            outfile << pair.first.experiment << "," << pair.first.lead_time << "," << pair.first.vt_hour << "," << pair.first.variable << "," << bias << "," << rmse << "," << pair.second.count << "\n";
        }
    }
    outfile.close();

    std::cout << "Saving temp metrics to temp_metrics.csv" << std::endl;
    std::ofstream temp_outfile("temp_metrics.csv", std::ios::trunc);
    temp_outfile.precision(6);
    temp_outfile << std::fixed << "experiment,lead_time,vt_hour,pressure_level,obstypevar,bias,rmse,n_samples\n";
    for(const auto& pair : final_temp_results) {
        if (pair.second.count > 0) {
            double bias = pair.second.sum_of_errors / pair.second.count;
            double rmse = std::sqrt(pair.second.sum_of_squared_errors / pair.second.count);
            temp_outfile << pair.first.experiment << "," << pair.first.lead_time << "," << pair.first.vt_hour << "," << pair.first.pressure_level << "," << pair.first.variable << "," << bias << "," << rmse << "," << pair.second.count << "\n";
        }
    }
    temp_outfile.close();

    auto script_end_time = std::chrono::high_resolution_clock::now();
    std::cout << "\n--- Total script execution time: " << std::chrono::duration<double>(script_end_time - script_start_time).count() << " seconds ---" << std::endl;

    return 0;
}
