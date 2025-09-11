<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Unified Verification Report</title>
    <link rel="stylesheet" href="css/style.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>Unified Verification Report</h1>
        </header>

        <nav>
            <div class="control-group">
                <label for="project-selector">Select Project:</label>
                <select id="project-selector">
                    <option value="">Loading projects...</option>
                </select>
            </div>
            <div class="control-group">
                <label for="variable-selector">Select Variable / Overview:</label>
                <select id="variable-selector">
                    <option value="">-- Select a project first --</option>
                </select>
            </div>
            <div class="control-group">
                <label for="plot-type-selector">Select Plot Type:</label>
                <select id="plot-type-selector">
                    <option value="">-- Select a variable first --</option>
                </select>
            </div>
        </nav>

        <main>
            <div id="plot-container">
                <img id="plot-display" src="" alt="Select a project and plot to display.">
            </div>
            <hr>
            <h2 id="scorecard-title">Scorecard Data</h2>
            <div id="scorecard-table-container">
                <p>Select a project to view scorecard data.</p>
            </div>
        </main>
    </div>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="js/main.js"></script>
</body>
</html>