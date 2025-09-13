<?php
// webapp/image.php

// The root directory where project data is stored.
// It's configured via an environment variable 'VERIF_DATA_PATH' when starting the PHP server.
$user_path = getenv('VERIF_DATA_PATH') ?: 'webapp/out/unified_verification';

// Handle both absolute and relative paths.
if (substr($user_path, 0, 1) === '/') {
    $data_root = realpath($user_path);
} else {
    $data_root = realpath(__DIR__ . '/../' . $user_path);
}

if ($data_root === false || !is_dir($data_root)) {
    header("HTTP/1.1 404 Not Found");
    echo "Data directory not found.";
    exit;
}

// Ensure trailing slash
$data_root = rtrim($data_root, '/') . '/';

// Get the requested image path from the query string
$image_path = $_GET['path'] ?? '';

// Basic security: prevent directory traversal
if (strpos($image_path, '..') !== false) {
    header("HTTP/1.1 400 Bad Request");
    echo "Invalid path.";
    exit;
}

// Construct the full path to the image
$full_path = $data_root . $image_path;

// Check if the file exists and is a file
if (file_exists($full_path) && is_file($full_path)) {
    // Get the file's mime type
    $mime_type = mime_content_type($full_path);
    // Set the content type header
    header('Content-Type: ' . $mime_type);
    // Output the file content
    readfile($full_path);
    exit;
} else {
    // Image not found
    header("HTTP/1.1 404 Not Found");
    echo "Image not found.";
    exit;
}
?>