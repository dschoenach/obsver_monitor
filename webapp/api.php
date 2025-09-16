<?php
header('Content-Type: application/json');

// The root directory where project data is stored.
// It's configured via an environment variable 'VERIF_DATA_PATH' when starting the PHP server.
$user_path = getenv('VERIF_DATA_PATH') ?: 'webapp/out/unified_verification';

// Handle both absolute and relative paths.
if (substr($user_path, 0, 1) === '/') {
    $data_root = realpath($user_path);
} else {
    $data_root = realpath(__DIR__ . '/../' . $user_path);
}

// Check if the directory exists
if ($data_root === false || !is_dir($data_root)) {
    echo json_encode(['error' => 'Verification data directory not found at: ' . htmlspecialchars($user_path) . '. Please set VERIF_DATA_PATH correctly.']);
    exit;
}

// Ensure trailing slash
$data_root = rtrim($data_root, '/') . '/';

$action = $_GET['action'] ?? 'get_projects';

$response = [];

// Load variable names mapping for display labels
$var_names = null;
$var_names_path = __DIR__ . '/var_names.json';
if (file_exists($var_names_path)) {
    $raw = file_get_contents($var_names_path);
    $decoded = json_decode($raw, true);
    if (is_array($decoded)) {
        $var_names = $decoded;
    }
}

if ($action === 'get_projects') {
    if (!is_dir($data_root)) {
        echo json_encode(['error' => 'Master output directory not found at: ' . realpath($data_root)]);
        exit;
    }

    $webapp_root_path = realpath(__DIR__);
    $projects = array_filter(scandir($data_root), function($item) use ($data_root) {
        return is_dir($data_root . $item) && !in_array($item, ['.', '..']);
    });

    foreach ($projects as $project) {
        $plots_dir = $data_root . $project . '/plots/';
        if (!is_dir($plots_dir)) continue;

        $response[$project] = [];

        // Scan all subdirectories for plots
        $subdirs = glob($plots_dir . '*', GLOB_ONLYDIR);

        $labels = [];
        foreach ($subdirs as $subdir) {
            $var_name = basename($subdir);
            if ($var_name === 'files') continue;

            if (!isset($response[$project][$var_name])) {
                $response[$project][$var_name] = [];
            }

            // Derive display label from mapping
            $display = $var_name;
            if ($var_names) {
                $group = 'surface';
                $code = $var_name;
                if (strpos($var_name, 'temp_') === 0) {
                    $group = 'upper_air';
                    $code = substr($var_name, 5);
                }
                if (isset($var_names[$group][$code])) {
                    $entry = $var_names[$group][$code];
                    if (is_array($entry) && isset($entry['label'])) {
                        $display = $entry['label'];
                    } elseif (is_string($entry)) {
                        $display = $entry;
                    }
                }
            }
            $labels[$var_name] = $display;

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

                $response[$project][$var_name][$cleaned_plot_type] = 'image.php?path=' . str_replace($data_root, '', $file);
            }
        }

        // Attach labels map for this project
        if (!empty($labels)) {
            $response[$project]['_var_labels'] = $labels;
        }

        // Find top-level plots (like scorecards)
        $top_level_plots = glob($plots_dir . '*.png');
        foreach ($top_level_plots as $file) {
            $filename = basename($file, '.png');
            if (stripos($filename, 'scorecard') !== false) {
                if (!isset($response[$project]['Scorecards'])) {
                    $response[$project]['Scorecards'] = [];
                }

                $imgPath = 'image.php?path=' . str_replace($data_root, '', $file);

                // For 'monitor' project, group surface + upper-air under a pair label like "REF vs. rednmc04"
                if ($project === 'monitor') {
                    // Expected filenames: monitor_surface_A_vs_B_scorecard.png or monitor_temp_A_vs_B_scorecard.png
                    if (preg_match('/^monitor_(surface|temp)_(.+)_scorecard$/i', $filename, $m)) {
                        $domain = strtolower($m[1]);
                        $pairRaw = $m[2]; // e.g., REF_vs_rednmc04
                        // Build display key with "vs." between names
                        $pairKey = str_replace('_vs_', ' vs. ', $pairRaw);
                        if (!isset($response[$project]['Scorecards'][$pairKey]) || !is_array($response[$project]['Scorecards'][$pairKey])) {
                            $response[$project]['Scorecards'][$pairKey] = [];
                        }
                        $domainKey = ($domain === 'surface') ? 'surface' : 'upper_air';
                        $response[$project]['Scorecards'][$pairKey][$domainKey] = $imgPath;
                        continue;
                    }
                }

                // Fallback: flat mapping (used by obsver or unknown patterns)
                // Clean common pattern: "Scorecard_<PAIR>_scorecard" => "<PAIR>"
                $displayKey = $filename;
                if (preg_match('/^Scorecard_(.+?)_scorecard$/i', $filename, $m)) {
                    $displayKey = $m[1];
                }
                $response[$project]['Scorecards'][$displayKey] = $imgPath;
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
