$(document).ready(function() {
    let projectData = {};
    let activeSelections = {
        project: null,
        category: null,
        variable: null
    };

    const categoryMap = {
        monitor: {
            'FCVER': 'lead_time_series',
            'FCTS': 'vt_hour_series',
            'TEMPPROF': 'profile'
        },
        obsver: {
            'PROF': 'profile',
            'TS': 'timeseries'
        }
    };

    // --- 1. Fetch ALL Project Data on page load ---
    $.getJSON('api.php?action=get_projects', function(data) {
        if (data.error) {
            $('#project-nav').html(`<p>${data.error}</p>`);
            return;
        }
        projectData = data;
        populateProjects(Object.keys(projectData));
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
        let categories = project === 'monitor' 
            ? ['FCVER', 'FCTS', 'TEMPPROF', 'Scorecards'] 
            : ['PROF', 'TS', 'Scorecards'];
        
        categories.forEach(c => {
            nav.append(`<a href="#" data-category="${c}">${c}</a>`);
        });
    }

    function populateVariables(project, category) {
        const nav = $('#variable-nav');
        nav.empty().append('<h2><a href="#" id="variables-header">Variables</a></h2>');
        const allVars = projectData[project] || {};
        let relevantVars = [];

        if (category === 'Scorecards') {
            relevantVars = Object.keys(allVars['Scorecards'] || {});
        } else {
            const plotType = categoryMap[project]?.[category];
            if (plotType) {
                Object.keys(allVars).forEach(variable => {
                    if (
                        variable !== 'Scorecards' &&
                        allVars[variable] &&
                        allVars[variable][plotType]
                    ) {
                        relevantVars.push(variable);
                    }
                });
            }
        }

        relevantVars.sort().forEach(v => {
            nav.append(`<a href="#" data-variable="${v}">${v}</a>`);
        });

        return relevantVars;
    }

    // Helper: find first category that has at least one variable (in order)
    function findFirstCategoryAndVariable(project) {
        const categories = project === 'monitor'
            ? ['FCVER', 'FCTS', 'TEMPPROF', 'Scorecards']
            : ['PROF', 'TS', 'Scorecards'];

        for (const cat of categories) {
            if (cat === 'Scorecards') {
                const scVars = Object.keys(projectData[project]?.Scorecards || {}).sort();
                if (scVars.length) return { category: cat, variable: scVars[0] };
            } else {
                const plotType = categoryMap[project]?.[cat];
                if (!plotType) continue;
                const vars = Object.keys(projectData[project] || {})
                    .filter(v => v !== 'Scorecards' && projectData[project][v]?.[plotType])
                    .sort();
                if (vars.length) return { category: cat, variable: vars[0] };
            }
        }
        return { category: null, variable: null };
    }

    // --- State Management & Rendering ---
    function setActive(type, value) {
        activeSelections[type] = value;

        switch (type) {
            case 'project':
                activeSelections.category = null;
                activeSelections.variable = null;
                populateCategories(value);
                fetchScorecardData(value);

                const first = findFirstCategoryAndVariable(value);
                if (first.category) {
                    activeSelections.category = first.category;
                    const vars = populateVariables(value, first.category);
                    // Trust the helper’s variable if still present, else fallback
                    activeSelections.variable = first.variable && vars.includes(first.variable)
                        ? first.variable
                        : (vars[0] || null);
                } else {
                    // No categories with variables
                    $('#variable-nav').empty().append('<h2>Variables</h2>');
                }
                break;

            case 'category':
                activeSelections.variable = null;
                const vars = populateVariables(activeSelections.project, value);
                if (vars.length) {
                    activeSelections.variable = vars[0];
                }
                break;

            case 'variable':
                // Direct selection, nothing else
                break;
        }

        renderUI();
    }

    function renderUI() {
        updateActiveLinks('#project-nav', 'project', 'project');
        updateActiveLinks('#category-nav', 'category', 'category');
        updateActiveLinks('#variable-nav', 'variable', 'variable');

        const plotContainer = $('#plot-container');
        plotContainer.empty();

        if (activeSelections.variable) {
            let imagePath;
            if (activeSelections.category === 'Scorecards') {
                imagePath = projectData[activeSelections.project]['Scorecards'][activeSelections.variable];
                $('#scorecard-table-container').show();
                $('#scorecard-title').show();
            } else {
                const plotType = categoryMap[activeSelections.project]?.[activeSelections.category];
                imagePath = projectData[activeSelections.project]?.[activeSelections.variable]?.[plotType];
                $('#scorecard-table-container').hide();
                $('#scorecard-title').hide();
            }

            if (imagePath) {
                const img = $('<img>').addClass('plot-display').attr('src', imagePath).show();
                plotContainer.append(img);
            } else {
                plotContainer.html('<p>Plot not found for this selection.</p>');
            }
        } else {
            plotContainer.html('<p>No variable available.</p>');
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
    $(document).on('click', '#project-nav a', function(e) { e.preventDefault(); setActive('project', $(this).data('project')); });
    $(document).on('click', '#category-nav a', function(e) { e.preventDefault(); setActive('category', $(this).data('category')); });
    $(document).on('click', '#variable-nav a', function(e) { e.preventDefault(); setActive('variable', $(this).data('variable')); });

    $(document).on('click', '#variables-header', function(e) {
        e.preventDefault();
        const { project, category } = activeSelections;
        if (!project || !category) return;

        const plotContainer = $('#plot-container');
        plotContainer.empty().css('text-align', 'left');
        let plotsFound = 0;

        $('#variable-nav a').each(function() {
            const variable = $(this).data('variable');
            let imagePath;

            if (category === 'Scorecards') {
                imagePath = projectData[project]['Scorecards'][variable];
            } else {
                const plotType = categoryMap[project]?.[category];
                imagePath = projectData[project]?.[variable]?.[plotType];
            }

            if (imagePath) {
                const plotTitle = $('<h3>').text(variable);
                const img = $('<img>').attr('src', imagePath).addClass('plot-display');
                plotContainer.append(plotTitle).append(img);
                plotsFound++;
            }
        });

        if (plotsFound === 0) {
            plotContainer.html('<p>No plots found for this group.</p>').css('text-align', 'center');
        }
    });

    $(document).on('click', '.plot-display', function() {
        $(this).toggleClass('zoomed');
    });

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