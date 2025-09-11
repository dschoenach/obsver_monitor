$(document).ready(function() {
    let projectData = {};

    // --- 1. Fetch ALL Project Data ---
    $.getJSON('api.php?action=get_projects', function(data) {
        if (data.error) {
            $('#project-selector').html(`<option>${data.error}</option>`);
            return;
        }
        projectData = data;
        populateProjectSelector(projectData);
    });

    function populateProjectSelector(data) {
        const projectSelector = $('#project-selector');
        projectSelector.empty().append('<option value="">-- Select a project --</option>');
        const sortedProjects = Object.keys(data).sort();
        $.each(sortedProjects, function(index, project) {
            projectSelector.append(`<option value="${project}">${project}</option>`);
        });
    }

    // --- 2. Handle Project Selection ---
    $('#project-selector').on('change', function() {
        const selectedProject = $(this).val();
        const varSelector = $('#variable-selector');
        
        // Reset everything downstream
        $('#plot-type-selector').empty().append('<option value="">-- Select a variable first --</option>');
        $('#plot-display').attr('src', '').hide();
        $('#scorecard-table-container').html('<p>Select a project to view scorecard data.</p>');
        $('#scorecard-title').text('Scorecard Data');

        if (!selectedProject) {
            varSelector.empty().append('<option value="">-- Select a project first --</option>');
            return;
        }

        // Populate variable selector
        varSelector.empty().append('<option value="">-- Select a variable --</option>');
        const variables = projectData[selectedProject];
        const sortedVars = Object.keys(variables).sort();
        $.each(sortedVars, function(index, variable) {
            varSelector.append(`<option value="${variable}">${variable}</option>`);
        });

        // Fetch this project's scorecard data
        fetchScorecardData(selectedProject);
    });

    // --- 3. Handle Variable Selection ---
    $('#variable-selector').on('change', function() {
        const selectedProject = $('#project-selector').val();
        const selectedVar = $(this).val();
        const plotTypeSelector = $('#plot-type-selector');
        
        $('#plot-display').attr('src', '').hide();
        if (!selectedVar) {
            plotTypeSelector.empty().append('<option value="">-- Select a variable first --</option>');
            return;
        }

        plotTypeSelector.empty().append('<option value="">-- Select a plot type --</option>');
        const plots = projectData[selectedProject][selectedVar];
        const sortedPlotTypes = Object.keys(plots).sort();
        $.each(sortedPlotTypes, function(index, plotType) {
            plotTypeSelector.append(`<option value="${plotType}">${plotType}</option>`);
        });
    });
    
    // --- 4. Handle Plot Type Selection ---
    $('#plot-type-selector').on('change', function() {
        const selectedProject = $('#project-selector').val();
        const selectedVar = $('#variable-selector').val();
        const selectedPlotType = $(this).val();

        if (!selectedPlotType) {
            $('#plot-display').attr('src', '').hide();
            return;
        }
        const imagePath = projectData[selectedProject][selectedVar][selectedPlotType];
        $('#plot-display').attr('src', imagePath).show();
    });

    // --- 5. Function to Fetch Scorecard Data ---
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