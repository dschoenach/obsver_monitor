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

        <div class="main-layout">
            <aside class="sidebar">
                <nav id="project-nav">
                    <h2>Projects</h2>
                    <!-- Projects will be loaded here as links -->
                </nav>
                <nav id="category-nav">
                    <h2>Categories</h2>
                    <!-- Categories will be loaded here -->
                </nav>
                <nav id="variable-nav">
                    <h2>Variables</h2>
                    <!-- Variables will be loaded here -->
                </nav>
                <nav id="plottype-nav">
                    <h2>Plot Types</h2>
                    <!-- Plot types will be loaded here -->
                </nav>
            </aside>

            <main class="content">
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
    </div>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="js/main.js"></script>
</body>
</html>
