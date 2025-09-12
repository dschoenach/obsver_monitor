<?php
header('Content-Type: application/json');

// The root directory where project data is stored.
$data_root = './out/unified_verification/';

$action = $_GET['action'] ?? 'get_projects';

$response = [];

if ($action === 'get_projects') {
    if (!is_dir($data_root)) {
        echo json_encode(['error' => 'Master output directory not found at: ' . realpath($data_root)]);
        exit;
    }

    $projects = array_filter(scandir($data_root), function($item) use ($data_root) {
        return is_dir($data_root . $item) && !in_array($item, ['.', '..']);
    });

    foreach ($projects as $project) {
        $plots_dir = $data_root . $project . '/plots/';
        if (!is_dir($plots_dir)) continue;

        $response[$project] = [];

        // Scan all subdirectories for plots
        $subdirs = glob($plots_dir . '*', GLOB_ONLYDIR);

        foreach ($subdirs as $subdir) {
            $var_name = basename($subdir);
            if ($var_name === 'files') continue;

            if (!isset($response[$project][$var_name])) {
                $response[$project][$var_name] = [];
            }

            foreach (glob($subdir . '/*.png') as $file) {
                $plot_type = basename($file, '.png');
                
                // More robust plot type cleaning
                $cleaned_plot_type = $plot_type;
                if (strpos($cleaned_plot_type, $var_name . '_') === 0) {
                    $cleaned_plot_type = substr($cleaned_plot_type, strlen($var_name) + 1);
                } else {
                    // Handle cases like temp_TT_profile where var_name is temp_TT
                    $cleaned_plot_type = str_replace($var_name, '', $cleaned_plot_type);
                    $cleaned_plot_type = ltrim($cleaned_plot_type, '_');
                }
                $cleaned_plot_type = str_replace('combined_', '', $cleaned_plot_type);

                $response[$project][$var_name][$cleaned_plot_type] = $file;
            }
        }

        // Find top-level plots (like scorecards)
        $top_level_plots = glob($plots_dir . '*.png');
        foreach ($top_level_plots as $file) {
            $filename = basename($file, '.png');
            if (stripos($filename, 'scorecard') !== false) {
                if (!isset($response[$project]['Scorecards'])) {
                    $response[$project]['Scorecards'] = [];
                }
                $response[$project]['Scorecards'][$filename] = $file;
            }
        }
    }

} elseif ($action === 'get_scorecard_data') {
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
