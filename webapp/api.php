<?php
header('Content-Type: application/json');

$data_root = './data/'; 
$action = $_GET['action'] ?? 'get_projects';

$response = [];

if ($action === 'get_projects') {
    $projects = array_filter(scandir($data_root), function($item) use ($data_root) {
        return is_dir($data_root . $item) && !in_array($item, ['.', '..']);
    });
    
    foreach ($projects as $project) {
        $plots_dir = $data_root . $project . '/plots/';
        if (!is_dir($plots_dir)) continue;

        $response[$project] = [];

        // --- NEW UNIFIED PLOT DISCOVERY LOGIC ---

        // 1. Find plots in subdirectories (like in 'obsver')
        $subdirs = glob($plots_dir . '*', GLOB_ONLYDIR);
        foreach ($subdirs as $subdir) {
            $var_name = basename($subdir);
            if ($var_name === 'files') continue;
            
            $response[$project][$var_name] = [];
            foreach (glob($subdir . '/*.png') as $file) {
                $plot_type = basename($file, '.png');
                $plot_type = str_replace($var_name . '_', '', $plot_type);
                $response[$project][$var_name][$plot_type] = $file;
            }
        }

        // 2. Find and categorize plots at the top level (like in 'monitor')
        $top_level_plots = glob($plots_dir . '*.png');
        foreach ($top_level_plots as $file) {
            $filename = basename($file, '.png');
            
            // If it's a scorecard, put it in the "Scorecards" category
            if (strpos(strtolower($filename), 'scorecard') !== false) {
                if (!isset($response[$project]['Scorecards'])) {
                    $response[$project]['Scorecards'] = [];
                }
                $response[$project]['Scorecards'][$filename] = $file;
            } 
            // Otherwise, parse the filename to create categories
            else {
                $parts = explode('_', $filename);
                $var_name = $parts[0];
                if (isset($parts[1]) && $parts[0] === 'temp') {
                    $var_name = 'temp_' . $parts[1]; // Handle cases like "temp_DD"
                }
                $plot_type = str_replace($var_name . '_', '', $filename);
                
                if (!isset($response[$project][$var_name])) {
                    $response[$project][$var_name] = [];
                }
                $response[$project][$var_name][$plot_type] = $file;
            }
        }
    }

} elseif ($action === 'get_scorecard_data') {
    // This part is unchanged and correct
    $project = $_GET['project'] ?? null;
    if (!$project) {
        $response = ['error' => 'Project name not provided.'];
    } else {
        $sqlite_db_path = $data_root . $project . '/plots/metrics.sqlite';
        if (!file_exists($sqlite_db_path)) {
            $response = ['error' => 'Metrics database not found for project: ' . htmlspecialchars($project)];
        } else {
            try {
                $pdo = new PDO('sqlite:' . $sqlite_db_path);
                $stmt = $pdo->query('SELECT * FROM scorecard_zscores ORDER BY obstypevar, lead_time');
                $response = $stmt->fetchAll(PDO::FETCH_ASSOC);
            } catch (PDOException $e) {
                $response = ['error' => 'Database query failed: ' . $e->getMessage()];
            }
        }
    }
}

echo json_encode($response, JSON_PRETTY_PRINT);
?>