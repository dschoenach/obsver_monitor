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

        <div id="topbar">
            <nav id="project-nav" class="project-row">
                <h2>Projects</h2>
                <!-- Projects will be loaded here as links -->
            </nav>
        </div>

        <div class="main-layout">
            <aside class="sidebar">
                <nav id="category-nav">
                    <h2>Categories</h2>
                    <!-- Categories will be loaded here -->
                </nav>
                <nav id="variable-nav">
                    <h2>Variables</h2>
                    <!-- Variables will be loaded here -->
                </nav>
                <nav id="plottype-nav" style="display: none;">
                    <h2>Plot Types</h2>
                    <!-- This is now hidden and unused -->
                </nav>
            </aside>

            <main class="content">
                <div id="plot-container">
                    <img id="plot-display" class="plot-display" src="" alt="Select a project and plot to display.">
                </div>
                <hr>
                <h2 id="scorecard-title">Scorecard Data</h2>
                <div id="scorecard-table-container">
                    <p>Select a project to view scorecard data.</p>
                </div>
            </main>
        </div>
    </div>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="js/main.js"></script>
</body>
</html>
