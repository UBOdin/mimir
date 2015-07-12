if (window.console) {
    console.log("Welcome to your Play application's JavaScript!");
}

$( document ).ready(function() {

    $(".table_link").on("click", function() {
        var table = $(this).html();
        if(table === "Lenses") {
            table = "MIMIR_LENSES";
        }

        var query = "SELECT * FROM "+table+";";

        $("#query_textarea").val(query);
        $("#query_btn").trigger("click");
    });

    $(".db_link").on("click", function() {
        var db = $(this).html();
        var curr = $("#curr_db").html().trim();

        if(db.valueOf() !== curr.valueOf()) {
            $("#db_field").val(db);
            $("#query_textarea").val("");
            $("#query_btn").trigger("click");
        }
    });

    $("#create_database").on("click", function() {
        var db = prompt("Please enter a name for the new database", "awesomedb");
        var existing_dbs = new Array();

        if(!db.match(/^\w+$/))
            alert("That is not a valid name, please try again");

        db += ".db";

        $(".db_link").each(function() {
            existing_dbs.push($(this).html());
        });

        if($.inArray(db, existing_dbs) != -1) {
            alert("A database with the name "+db+" already exists");
        }
        else {
            $("#create_db_field").val(db);
            $("#create_db_form").submit();
        }
    });

    $("#result_table").colResizable( {
        liveDrag: true,
        minWidth: 80
    });

    $("#about_btn").on("click", function() {
        $("#about").toggle(100);
    });

    $("#upload").on("click", function() {
        $("#drop_area").toggle(100);
    });

    $(".close_btn").on("click", function() {
        $(this).parent().hide(100);
    });



    $(".non_deterministic_cell").one("click", function(e) {
        $(this).tooltipster({
            animation: 'fade',
            delay: 10,
            contentAsHTML: 'true',
            functionInit: function(origin, content) {

            		var col = origin.parents('table').find('th').eq(origin.prevAll().length).text();
                    var row = origin.parent().children(".rowid_col").html();
                    var query = $("#query_textarea").val().replace(";","");
                    var table = extractTable(query);

                    var bounds = [];
                    var variance;
                    var conf_int = [];
                    var causes = ['Table R has missing value for attribute \'B\'', 'Another cause'];

                    if (content == null) {

                        $.ajax({
                            type: 'GET',
                            url: 'queryjson?query=SELECT BOUNDS('+col+'), VAR('+col+'), CONF('+col+') FROM ('+query+') AS TEMP WHERE ROWID = '+row+';&db=debug.db',
                            success: function(res) {
                                bounds[0] = res.data[0][1];
                                bounds[1] = res.data[0][2];
                                variance = res.data[0][3];
                                conf_int[0] = res.data[0][4];
                                conf_int[1] = res.data[0][5];

                                var tooltip_template = '<table class="table tooltip_table">'+
                                                          '<tbody>'+
                                                              '<tr>'+
                                                                  '<th scope="row">Bounds</th>'+
                                                                  '<td class="number">'+ bounds[0] +' - '+ bounds[1] +'</td>'+
                                                              '</tr>'+
                                                              '<tr>'+
                                                                  '<th scope="row">Variance</th>'+
                                                                  '<td class="number">'+ variance +'</td>'+
                                                              '</tr>'+
                                                              '<tr>'+
                                                                  '<th scope="row">Confidence Interval</th>'+
                                                                  '<td class="number">'+ conf_int[0] + ' - '+ conf_int[1] +'</td>'+
                                                              '</tr>'+
                                                              '<tr>'+
                                                                  '<th scope="row">Causes</th>'+
                                                                  '<td><ul>'+ listify(causes) +'</ul></td>'+
                                                              '</tr>'+
                                                          '</tbody>'+
                                                      '</table>';
                                origin.tooltipster('content', tooltip_template);
                            }
                        });

                        // this returned string will overwrite the content of the tooltip for the time being
                        return 'Loading...';
                    }
                    else {
                        // return nothing : the initialization continues normally with its content unchanged.
                    }
                },
            theme: 'tooltipster-shadow',
            position: 'bottom',
            minWidth: 350,
            maxWidth: 350,
            trigger: 'click',
        });

        $(this).click();
    });

    $("[name='db'").val($("#curr_db").html());

    Dropzone.options.myAwesomeDropzone = {
      maxFilesize: 2, // MB
      acceptedFiles: ".csv",
      addRemoveLinks: true,
      init: function() {
        this.on("error", function() {
            var span = $("span[data-dz-errormessage]");
            span.html("There is no table with this name in the current database!");
        });
      }
    };

});

function listify(causes) {
    var i;
    var result = '';
    for(i = 0; i<causes.length; i++) {
        result += '<li class="repair">'+ causes[i] +'</li>'
    }

    return result;
}

function extractTable(query) {
    return query.match(/FROM[\s]+(\w+)(\s*,\s*\w+)*/i)[0].substring(4).trim();
}