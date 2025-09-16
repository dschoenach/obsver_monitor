// FileUtils.cpp
#include "FileUtils.hpp"
#include "DateTimeUtils.hpp"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <regex>
#include <iostream>
#include <unordered_map>

namespace fs = std::filesystem;

FileInfo parse_filename(const std::string& path) {
    std::string basename = fs::path(path).filename().string();
    FileInfo info;
    info.path = path;
    
    if (basename.rfind("vfld", 0) == 0) {
        std::smatch m;
        std::regex re("(20\\d{6})(\\d{2})(\\d{2})$");
        if (std::regex_search(basename, m, re)) {
            info.type = "vfld";
            info.base_time = std::stoll(m[1].str() + m[2].str());
            info.lead_time = std::stoi(m[3].str());
            info.valid_time = add_hours_to_yyyymmddhh(info.base_time, info.lead_time);
        }
    } else if (basename.rfind("vobs", 0) == 0) {
        std::smatch m;
        std::regex re("(20\\d{6})(\\d{2})$");
        if (std::regex_search(basename, m, re)) {
            info.type = "vobs";
            info.base_time = std::stoll(m[1].str() + m[2].str());
            info.valid_time = info.base_time;
            info.experiment = "observation";
        }
    }
    return info;
}

void read_data_file(const std::string& filepath, bool is_vfld, int& version_flag,
                    std::vector<SurfaceStation>& stations, std::vector<TempLevel>& temp_levels) {
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
            if(!std::getline(file, line)) return;
            int ninvar = std::stoi(line);
            for (int i=0; i<ninvar; ++i) { if(!std::getline(file, line)) break; std::stringstream ss(line); std::string vn; ss >> vn; surface_variables.push_back(vn); }
        }
        std::unordered_map<std::string, int> surface_col_map;
        for (size_t i=0; i<surface_variables.size(); ++i) surface_col_map[surface_variables[i]] = i;
        
        stations.reserve(num_stat);
        for (int i=0; i<num_stat; ++i) {
            if (!std::getline(file, line) || line.empty()) break;
            std::stringstream line_stream(line);
            SurfaceStation s={};
            line_stream >> s.id >> s.lat >> s.lon;
            if (!is_vfld) { line_stream >> s.hgt; }
            
            std::vector<double> data_values;
            double val;
            while(line_stream >> val) data_values.push_back(val);
            
            if (is_vfld) {
                if (surface_col_map.count("FI") && static_cast<size_t>(surface_col_map.at("FI")) < data_values.size()) s.hgt=data_values[surface_col_map.at("FI")];
                else if (surface_col_map.count("hgt") && static_cast<size_t>(surface_col_map.at("hgt")) < data_values.size()) s.hgt=data_values[surface_col_map.at("hgt")];
            }
            if (surface_col_map.count("NN") && static_cast<size_t>(surface_col_map.at("NN")) < data_values.size()) s.nn=data_values[surface_col_map.at("NN")];
            if (surface_col_map.count("DD") && static_cast<size_t>(surface_col_map.at("DD")) < data_values.size()) s.dd=data_values[surface_col_map.at("DD")];
            if (surface_col_map.count("FF") && static_cast<size_t>(surface_col_map.at("FF")) < data_values.size()) s.ff=data_values[surface_col_map.at("FF")];
            if (surface_col_map.count("TT") && static_cast<size_t>(surface_col_map.at("TT")) < data_values.size()) s.tt=data_values[surface_col_map.at("TT")];
            if (surface_col_map.count("RH") && static_cast<size_t>(surface_col_map.at("RH")) < data_values.size()) s.rh=data_values[surface_col_map.at("RH")];
            if (surface_col_map.count("PS") && static_cast<size_t>(surface_col_map.at("PS")) < data_values.size()) s.ps=data_values[surface_col_map.at("PS")];
            // Station pressure may be provided as SPS or PSS in files; accept both
            if (surface_col_map.count("SPS") && (size_t)surface_col_map.at("SPS") < data_values.size()) s.pss = data_values[surface_col_map.at("SPS")];
            else if (surface_col_map.count("PSS") && (size_t)surface_col_map.at("PSS") < data_values.size()) s.pss = data_values[surface_col_map.at("PSS")];
            if (surface_col_map.count("PE")  && static_cast<size_t>(surface_col_map.at("PE"))  < data_values.size()) s.pe = data_values[surface_col_map.at("PE")];
            if (surface_col_map.count("PE1") && static_cast<size_t>(surface_col_map.at("PE1")) < data_values.size()) s.pe1 = data_values[surface_col_map.at("PE1")];
            if (surface_col_map.count("PE3") && static_cast<size_t>(surface_col_map.at("PE3")) < data_values.size()) s.pe3 = data_values[surface_col_map.at("PE3")];
            if (surface_col_map.count("PE6") && static_cast<size_t>(surface_col_map.at("PE6")) < data_values.size()) s.pe6 = data_values[surface_col_map.at("PE6")];
            if (surface_col_map.count("PE12")&& static_cast<size_t>(surface_col_map.at("PE12"))< data_values.size()) s.pe12= data_values[surface_col_map.at("PE12")];
            if (surface_col_map.count("PE24")&& static_cast<size_t>(surface_col_map.at("PE24"))< data_values.size()) s.pe24= data_values[surface_col_map.at("PE24")];
            if (surface_col_map.count("QQ") && static_cast<size_t>(surface_col_map.at("QQ")) < data_values.size()) s.qq=data_values[surface_col_map.at("QQ")];
            if (surface_col_map.count("VI") && static_cast<size_t>(surface_col_map.at("VI")) < data_values.size()) s.vi=data_values[surface_col_map.at("VI")];
            if (surface_col_map.count("TD") && static_cast<size_t>(surface_col_map.at("TD")) < data_values.size()) s.td=data_values[surface_col_map.at("TD")];
            if (surface_col_map.count("TX") && static_cast<size_t>(surface_col_map.at("TX")) < data_values.size()) s.tx=data_values[surface_col_map.at("TX")];
            if (surface_col_map.count("TN") && static_cast<size_t>(surface_col_map.at("TN")) < data_values.size()) s.tn=data_values[surface_col_map.at("TN")];
            if (surface_col_map.count("GG") && static_cast<size_t>(surface_col_map.at("GG")) < data_values.size()) s.gg=data_values[surface_col_map.at("GG")];
            if (surface_col_map.count("GX") && static_cast<size_t>(surface_col_map.at("GX")) < data_values.size()) s.gx=data_values[surface_col_map.at("GX")];
            if (surface_col_map.count("FX") && static_cast<size_t>(surface_col_map.at("FX")) < data_values.size()) s.fx=data_values[surface_col_map.at("FX")];
            if (surface_col_map.count("TTHA") && static_cast<size_t>(surface_col_map.at("TTHA")) < data_values.size()) s.ttha=data_values[surface_col_map.at("TTHA")];
            if (surface_col_map.count("CH") && static_cast<size_t>(surface_col_map.at("CH")) < data_values.size()) s.ch=data_values[surface_col_map.at("CH")];
            if (surface_col_map.count("LC") && static_cast<size_t>(surface_col_map.at("LC")) < data_values.size()) s.lc=data_values[surface_col_map.at("LC")];
            stations.push_back(s);
        }

        if (num_temp > 0) {
            if(!std::getline(file, line) || line.empty()) return;
            int num_temp_lev = std::stoi(line);
            
            if(!std::getline(file, line) || line.empty()) return;
            int ninvar_temp = std::stoi(line);
            
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
                    std::stringstream level_ss(line);
                    std::vector<double> values;
                    double val;
                    while(level_ss >> val) values.push_back(val);
                    if (values.empty()) continue;
                    
                    TempLevel tl;
                    tl.station_id = station_id;
                    if (temp_col_map.count("PP") && static_cast<size_t>(temp_col_map.at("PP")) < values.size()) tl.pressure=values[temp_col_map.at("PP")];
                    if (temp_col_map.count("TT") && static_cast<size_t>(temp_col_map.at("TT")) < values.size()) tl.temp=values[temp_col_map.at("TT")];
                    if (temp_col_map.count("FI") && static_cast<size_t>(temp_col_map.at("FI")) < values.size()) tl.fi=values[temp_col_map.at("FI")];
                    if (temp_col_map.count("TD") && static_cast<size_t>(temp_col_map.at("TD")) < values.size()) tl.td=values[temp_col_map.at("TD")];
                    if (temp_col_map.count("RH") && static_cast<size_t>(temp_col_map.at("RH")) < values.size()) tl.rh=values[temp_col_map.at("RH")];
                    if (temp_col_map.count("QQ") && static_cast<size_t>(temp_col_map.at("QQ")) < values.size()) tl.qq=values[temp_col_map.at("QQ")];
                    if (temp_col_map.count("DD") && static_cast<size_t>(temp_col_map.at("DD")) < values.size()) tl.dd=values[temp_col_map.at("DD")];
                    if (temp_col_map.count("FF") && static_cast<size_t>(temp_col_map.at("FF")) < values.size()) tl.ff=values[temp_col_map.at("FF")];
                    temp_levels.push_back(tl);
                }
            }
        }
    } catch (const std::exception& e) {
        #pragma omp critical
        std::cerr << "Warning: Exception caught while reading " << filepath << ": " << e.what() << ". Skipping file." << std::endl;
    }
}
