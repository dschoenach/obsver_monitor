$(document).ready(function() {
    let projectData = {};
    let activeSelections = {
        project: null,
        category: null,
        variable: null,
        plotType: null
    };

    // --- 1. Fetch ALL Project Data on page load ---
    $.getJSON('api.php?action=get_projects', function(data) {
        if (data.error) {
            $('#project-nav').html(`<p>${data.error}</p>`);
            return;
        }
        projectData = data;
        
        // Initial population of the page
        populateProjects(Object.keys(projectData));
        
        // Set default project to 'monitor' or the first available one
        const defaultProject = projectData.monitor ? 'monitor' : Object.keys(projectData)[0];
        if (defaultProject) {
            setActive('project', defaultProject);
        }
    });

    // --- UI Population Functions ---
    function populateProjects(projects) {
        const nav = $('#project-nav');
        nav.empty().append('<h2>Projects</h2>');
        projects.sort().forEach(p => {
            nav.append(`<a href="#" data-project="${p}">${p}</a>`);
        });
    }

    function populateCategories(project) {
        const nav = $('#category-nav');
        nav.empty().append('<h2>Categories</h2>');
        // Define static categories
        const categories = ['Synop_Surface', 'Temp_Profiles', 'Scorecards']; 
        categories.forEach(c => {
            nav.append(`<a href="#" data-category="${c}">${c}</a>`);
        });
    }

    function populateVariables(project, category) {
        const nav = $('#variable-nav');
        nav.empty().append('<h2>Variables</h2>');
        const allVars = projectData[project] || {};
        let relevantVars = [];

        if (category === 'Scorecards') {
            if (allVars['Scorecards']) {
                relevantVars = ['Scorecards'];
            }
        } else if (category === 'Synop_Surface') {
            relevantVars = Object.keys(allVars).filter(v => v !== 'Scorecards' && !v.toLowerCase().includes('temp'));
        } else if (category === 'Temp_Profiles') {
            relevantVars = Object.keys(allVars).filter(v => v.toLowerCase().includes('temp'));
        }

        relevantVars.sort().forEach(v => {
            nav.append(`<a href="#" data-variable="${v}">${v}</a>`);
        });
    }

    function populatePlotTypes(project, variable) {
        const nav = $('#plottype-nav');
        nav.empty().append('<h2>Plot Types</h2>');
        const plots = projectData[project]?.[variable] || {};
        Object.keys(plots).sort().forEach(pt => {
            nav.append(`<a href="#" data-plottype="${pt}">${pt}</a>`);
        });
    }

    // --- State Management & Rendering ---
    function setActive(type, value) {
        console.log(`Setting active ${type}: ${value}`);
        activeSelections[type] = value;

        // Cascade resets and updates
        switch (type) {
            case 'project':
                activeSelections.category = null;
                activeSelections.variable = null;
                activeSelections.plotType = null;
                populateCategories(value);
                fetchScorecardData(value);
                break;
            case 'category':
                activeSelections.variable = null;
                activeSelections.plotType = null;
                populateVariables(activeSelections.project, value);
                break;
            case 'variable':
                activeSelections.plotType = null;
                populatePlotTypes(activeSelections.project, value);
                break;
            case 'plotType':
                break; // No downstream updates
        }
        renderUI();
    }

    function renderUI() {
        // Update active classes for links
        updateActiveLinks('#project-nav', 'project', 'project');
        updateActiveLinks('#category-nav', 'category', 'category');
        updateActiveLinks('#variable-nav', 'variable', 'variable');
        updateActiveLinks('#plottype-nav', 'plottype', 'plotType');

        // Auto-select first item if not set
        if (activeSelections.project && !activeSelections.category) {
            const firstCategory = $('#category-nav a:first').data('category');
            if (firstCategory) setActive('category', firstCategory);
            return; // Return to allow cascade
        }
        if (activeSelections.category && !activeSelections.variable) {
            const firstVar = $('#variable-nav a:first').data('variable');
            if (firstVar) setActive('variable', firstVar);
            return; 
        }
        if (activeSelections.variable && !activeSelections.plotType) {
            const firstPlot = $('#plottype-nav a:first').data('plottype');
            if (firstPlot) setActive('plotType', firstPlot);
            return;
        }

        // Display plot if everything is selected
        if (activeSelections.project && activeSelections.variable && activeSelections.plotType) {
            const imagePath = projectData[activeSelections.project][activeSelections.variable][activeSelections.plotType];
            $('#plot-display').attr('src', imagePath).show();
        } else {
            $('#plot-display').attr('src', '').hide();
        }
    }
    
    function updateActiveLinks(navId, dataAttr, selectionType) {
        $(`${navId} a`).each(function() {
            if ($(this).data(dataAttr) === activeSelections[selectionType]) {
                $(this).addClass('active');
            } else {
                $(this).removeClass('active');
            }
        });
    }

    // --- Event Handlers ---
    $(document).on('click', '#project-nav a', function(e) {
        e.preventDefault();
        setActive('project', $(this).data('project'));
    });
    $(document).on('click', '#category-nav a', function(e) {
        e.preventDefault();
        setActive('category', $(this).data('category'));
    });
    $(document).on('click', '#variable-nav a', function(e) {
        e.preventDefault();
        setActive('variable', $(this).data('variable'));
    });
    $(document).on('click', '#plottype-nav a', function(e) {
        e.preventDefault();
        setActive('plotType', $(this).data('plottype'));
    });

    // --- Data Fetching ---
    function fetchScorecardData(projectName) {
        $('#scorecard-title').text(`Scorecard Data for: ${projectName}`);
        $.getJSON(`api.php?action=get_scorecard_data&project=${projectName}`, function(data) {
            const container = $('#scorecard-table-container');
            if (data.error || data.length === 0) {
                container.html(`<p>${data.error || 'No scorecard data found for this project.'}</p>`);
                return;
            }
            const table = $('<table>');
            $('<thead>').appendTo(table).append($('<tr>').append(Object.keys(data[0]).map(key => `<th>${key}</th>`)));
            const tbody = $('<tbody>').appendTo(table);
            data.forEach(rowData => {
                const row = $('<tr>').appendTo(tbody);
                Object.values(rowData).forEach(value => {
                    const cellValue = (typeof value === 'number') ? value.toFixed(4) : value;
                    $('<td>').text(cellValue).appendTo(row);
                });
            });
            container.empty().append(table);
        });
    }
});
