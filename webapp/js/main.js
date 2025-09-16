$(document).ready(function() {
    let projectData = {};
    let activeSelections = {
        project: null,
        category: null,
        variable: null
    };
    let showAllVariables = false;
    let previousVariable = null;

    const categoryMap = {
        monitor: {
            'FCVER_SURF': 'lead_time_series',
            'FCVER_VERT': 'lead_time_series',
            'FCTS_SURF': 'vt_hour_series',
            'FCTS_VERT': 'vt_hour_series',
            'TEMPPROF': 'profile'
        },
        obsver: {
            'PROF': 'profile',
            'TS': 'timeseries'
        }
    };

    // Variable order for monitor categories; can be overridden by window.MONITOR_VAR_ORDER (static bundles)
    const monitorSurfOrder = (window.MONITOR_VAR_ORDER && window.MONITOR_VAR_ORDER.surf)
        ? window.MONITOR_VAR_ORDER.surf
        : ['PS','SPS','FF','GX','DD','TT','TTHA','TN','TX','TD','TDD','RH','QQ','NN','LC','CH','PE1','PE3','PE6','PE12','VI'];
    const monitorVertOrder = (window.MONITOR_VAR_ORDER && window.MONITOR_VAR_ORDER.temp)
        ? window.MONITOR_VAR_ORDER.temp
        : ['TT','TD','FF','DD','FI','RH','QQ'];
    function sortByFixedOrder(vars, project, category, labels) {
        if (project !== 'monitor') {
            // Default: label sort for non-monitor
            return vars.sort((a, b) => (labels[a] || a).localeCompare(labels[b] || b));
        }
        const wantsVertical = categoryWantsVertical(project, category);
        const orderArr = wantsVertical ? monitorVertOrder : monitorSurfOrder;
        const index = new Map(orderArr.map((v, i) => [v, i]));
        return vars.sort((a, b) => {
            const ia = index.has(a) ? index.get(a) : Number.MAX_SAFE_INTEGER;
            const ib = index.has(b) ? index.get(b) : Number.MAX_SAFE_INTEGER;
            if (ia !== ib) return ia - ib;
            // Fallback to label
            return (labels[a] || a).localeCompare(labels[b] || b);
        });
    }

    const categoryDisplay = {
        monitor: {
            'FCVER_SURF': 'Verf. by LeadTime (Surface)',
            'FCVER_VERT': 'Verf. by LeadTime (Vertical)',
            'FCTS_SURF': 'Timeseries (Surface)',
            'FCTS_VERT': 'Timeseries (Vertical)',
            'TEMPPROF': 'Vert. Profiles'
        },
        obsver: {
            'PROF': 'Vert. Profiles',
            'TS': 'Timeseries'
        }
    };

    function isVerticalVar(varName) {
        return typeof varName === 'string' && varName.startsWith('temp_');
    }

    function categoryWantsVertical(project, category) {
        if (project !== 'monitor') return null;
        return ['FCVER_VERT', 'FCTS_VERT', 'TEMPPROF'].includes(category) ? true
             : ['FCVER_SURF', 'FCTS_SURF'].includes(category) ? false
             : null;
    }

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
            ? ['Scorecards', 'FCVER_SURF', 'FCVER_VERT', 'FCTS_SURF', 'FCTS_VERT', 'TEMPPROF'] 
            : ['Scorecards', 'PROF', 'TS'];
        
        categories.forEach(c => {
            const label = c === 'Scorecards' ? 'Scorecards' : (categoryDisplay[project]?.[c] || c);
            nav.append(`<a href="#" data-category="${c}">${label}</a>`);
        });
    }

    function populateVariables(project, category) {
        const nav = $('#variable-nav');
        const btnLabel = showAllVariables ? 'Hide All' : 'Show All';
        nav.empty().append(`<h2>Variables <button id="variables-toggle" type="button" class="btn-variables">${btnLabel}</button></h2>`);
        const allVars = projectData[project] || {};
        const labels = allVars['_var_labels'] || {};
        let relevantVars = [];

        if (category === 'Scorecards') {
            relevantVars = Object.keys(allVars['Scorecards'] || {});
        } else {
            const plotType = categoryMap[project]?.[category];
            if (plotType) {
                const wantsVertical = categoryWantsVertical(project, category);
                Object.keys(allVars).forEach(variable => {
                    if (
                        variable !== 'Scorecards' &&
                        !variable.startsWith('_') &&
                        allVars[variable] &&
                        allVars[variable][plotType] &&
                        (wantsVertical === null || isVerticalVar(variable) === wantsVertical)
                    ) {
                        relevantVars.push(variable);
                    }
                });
            }
        }

        // Sort according to fixed order for monitor; else by label
        relevantVars = sortByFixedOrder(relevantVars, project, category, labels);
        relevantVars.forEach(v => {
            const label = labels[v] || v;
            nav.append(`<a href="#" data-variable="${v}">${label}</a>`);
        });

        return relevantVars;
    }

    // Helper: find first category that has at least one variable (in order)
    function findFirstCategoryAndVariable(project) {
        const categories = project === 'monitor'
            ? ['Scorecards', 'FCVER_SURF', 'FCVER_VERT', 'FCTS_SURF', 'FCTS_VERT', 'TEMPPROF']
            : ['Scorecards', 'PROF', 'TS'];

        for (const cat of categories) {
            if (cat === 'Scorecards') {
                const scVars = Object.keys(projectData[project]?.Scorecards || {}).sort();
                if (scVars.length) return { category: cat, variable: scVars[0] };
            } else {
                const plotType = categoryMap[project]?.[cat];
                if (!plotType) continue;
                const wantsVertical = categoryWantsVertical(project, cat);
                const vars = Object.keys(projectData[project] || {})
                    .filter(v => v !== 'Scorecards' && !v.startsWith('_') && projectData[project][v]?.[plotType]
                        && (wantsVertical === null || isVerticalVar(v) === wantsVertical))
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
                showAllVariables = false;
                previousVariable = null;
                populateCategories(value);
                fetchScorecardData(value);

                const first = findFirstCategoryAndVariable(value);
                if (first.category) {
                    activeSelections.category = first.category;
                    const vars = populateVariables(value, first.category);
                    // Always default to the first item in the rendered list
                    activeSelections.variable = (vars[0] || null);
                } else {
                    // No categories with variables
                    $('#variable-nav').empty().append('<h2>Variables</h2>');
                }
                break;

            case 'category':
                activeSelections.variable = null;
                showAllVariables = false;
                previousVariable = null;
                const vars = populateVariables(activeSelections.project, value);
                if (vars.length) {
                    activeSelections.variable = vars[0];
                }
                break;

            case 'variable':
                // Direct selection, nothing else
                showAllVariables = false;
                previousVariable = value;
                break;
        }

        renderUI();

        // Update background color based on project selection
        const bg = (activeSelections.project === 'obsver') ? '#ebebeb' : '#ffffcc';
        $('.container').css('background-color', bg);
    }

    function renderUI() {
        updateActiveLinks('#project-nav', 'project', 'project');
        updateActiveLinks('#category-nav', 'category', 'category');
        updateActiveLinks('#variable-nav', 'variable', 'variable');

        const plotContainer = $('#plot-container');
        plotContainer.empty();

        if (activeSelections.variable) {
            if (activeSelections.category === 'Scorecards') {
                const entry = projectData[activeSelections.project]['Scorecards'][activeSelections.variable];
                $('#scorecard-table-container').show();
                $('#scorecard-title').show();

                plotContainer.empty();
                if (entry && typeof entry === 'object') {
                    // Render both Surface and Upper Air (stacked) when available
                    if (entry.surface) {
                        plotContainer.append($('<h3>').text('Surface'));
                        plotContainer.append($('<img>').addClass('plot-display').attr('src', entry.surface));
                    }
                    if (entry.upper_air) {
                        plotContainer.append($('<h3>').text('Upper Air'));
                        plotContainer.append($('<img>').addClass('plot-display').attr('src', entry.upper_air));
                    }
                    if (!entry.surface && !entry.upper_air) {
                        plotContainer.html('<p>Scorecard not found for this selection.</p>');
                    }
                } else {
                    // Backward compatibility: single URL
                    if (entry) {
                        const img = $('<img>').addClass('plot-display').attr('src', entry).show();
                        plotContainer.append(img);
                    } else {
                        plotContainer.html('<p>Scorecard not found for this selection.</p>');
                    }
                }
            } else {
                const plotType = categoryMap[activeSelections.project]?.[activeSelections.category];
                const imagePath = projectData[activeSelections.project]?.[activeSelections.variable]?.[plotType];
                $('#scorecard-table-container').hide();
                $('#scorecard-title').hide();
                if (imagePath) {
                    const img = $('<img>').addClass('plot-display').attr('src', imagePath).show();
                    plotContainer.append(img);
                } else {
                    plotContainer.html('<p>Plot not found for this selection.</p>');
                }
            }
        } else {
            plotContainer.html('<p>No variable available.</p>');
        }
    }
    
    function updateActiveLinks(navId, dataAttr, selectionType) {
        const isVariableNav = (navId === '#variable-nav');
        $(`${navId} a`).each(function() {
            const matches = $(this).data(dataAttr) === activeSelections[selectionType];
            const shouldActive = isVariableNav && showAllVariables ? true : matches;
            $(this).toggleClass('active', shouldActive);
        });
    }

    // --- Event Handlers ---
    $(document).on('click', '#project-nav a', function(e) { e.preventDefault(); setActive('project', $(this).data('project')); });
    $(document).on('click', '#category-nav a', function(e) { e.preventDefault(); setActive('category', $(this).data('category')); });
    $(document).on('click', '#variable-nav a', function(e) { e.preventDefault(); setActive('variable', $(this).data('variable')); });

    $(document).on('click', '#variables-toggle', function(e) {
        e.preventDefault();
        const { project, category } = activeSelections;
        if (!project || !category) return;

        // Toggle off: revert to single selection using current active variable
        if (showAllVariables) {
            showAllVariables = false;
            // Rebuild variable list (label-sorted) and update button label,
            // then restore previous variable if possible; else pick first in list.
            const vars = populateVariables(project, category);
            if (previousVariable && vars.includes(previousVariable)) {
                activeSelections.variable = previousVariable;
            } else {
                activeSelections.variable = vars[0] || null;
                previousVariable = activeSelections.variable;
            }
            renderUI();
            return;
        }

        // Toggle on: show all variables in this category
        // Remember current variable to restore after hiding all
        previousVariable = activeSelections.variable;
        const plotContainer = $('#plot-container');
        plotContainer.empty().css('text-align', 'left');
        let plotsFound = 0;

        $('#variable-nav a').each(function() {
            const variable = $(this).data('variable');

            if (category === 'Scorecards') {
                const entry = projectData[project]['Scorecards'][variable];
                if (entry) {
                    const sectionTitle = $('<h3>').text(variable);
                    plotContainer.append(sectionTitle);
                    if (typeof entry === 'object') {
                        if (entry.surface) {
                            plotContainer.append($('<h4>').text('Surface'));
                            plotContainer.append($('<img>').attr('src', entry.surface).addClass('plot-display'));
                            plotsFound++;
                        }
                        if (entry.upper_air) {
                            plotContainer.append($('<h4>').text('Upper Air'));
                            plotContainer.append($('<img>').attr('src', entry.upper_air).addClass('plot-display'));
                            plotsFound++;
                        }
                    } else if (typeof entry === 'string') {
                        plotContainer.append($('<img>').attr('src', entry).addClass('plot-display'));
                        plotsFound++;
                    }
                }
            } else {
                const plotType = categoryMap[project]?.[category];
                const imagePath = projectData[project]?.[variable]?.[plotType];
                if (imagePath) {
                    const plotTitle = $('<h3>').text(variable);
                    const img = $('<img>').attr('src', imagePath).addClass('plot-display');
                    plotContainer.append(plotTitle).append(img);
                    plotsFound++;
                }
            }
        });

        // Mark all displayed variables as active in the nav
        showAllVariables = true;
        updateActiveLinks('#variable-nav', 'variable', 'variable');
        // Update button label
        $('#variables-toggle').text('Hide All');

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
