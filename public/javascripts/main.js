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
        minWidth: 20
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
            animation: 'grow',
            delay: 10,
            contentAsHTML: 'true',
            functionInit: function(origin, content) {

                    var col_index = origin.prevAll().length;
            		var col = origin.parents('table').find('th').eq(col_index).text();
                    var row = origin.parent().children(".rowid_col").html();
                    var query = $("#query_textarea").val().replace(";","");
                    var db = $("#db_field").val();

                    var bounds = [];
                    var variance;
                    var conf_int = [];
                    var causes = [];

                    var data_params_query = 'queryjson?query=SELECT BOUNDS('+col
                                            +'), VAR('+col+'), CONF('+col+') FROM ('+query
                                            +') AS TEMP WHERE ROWID = '+row+';&db='+db;

                    var vgterms_query = 'queryjson?query=EXPLAIN '+query+';&db='+db;

                    if (content == null) {

                        $.when(
                            $.get(data_params_query, function (res) {
                                bounds[0] = res.data[0][1];
                                bounds[1] = res.data[0][2];
                                variance = res.data[0][3];
                                conf_int[0] = res.data[0][4];
                                conf_int[1] = res.data[0][5];
                            }),
                            $.get(vgterms_query, function (res) {
                                causes = extractCauses(res.result, col_index);
                            })
                        ).then( function() {
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
                                                              '<th scope="row">VG Terms</th>'+
                                                              '<td><ul>'+ listify(causes) +'</ul></td>'+
                                                          '</tr>'+
                                                      '</tbody>'+
                                                  '</table>';
                            origin.tooltipster('content', tooltip_template);
                        });

                        // this returned string will overwrite the content of the tooltip for the time being
                        return '<b>Loading...</b>';
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

    $(".non_deterministic_row").one("click", function(e) {
        $(this).tooltipster({
            animation: 'grow',
            delay: 10,
            functionInit: function(origin, content) {

                var query = $("#query_textarea").val().replace(";","");
                var db = $("#db_field").val();

                var causes = [];

                var vgterms_query = 'queryjson?query=EXPLAIN '+query+';&db='+db;

                if (content == null) {
                    $.when(
                        $.get(vgterms_query, function (res) {
                            causes = extractCauses(res.result, -1);
                            // -1 will indicate __MIMIR_CONDITION
                        })
                    ).then( function() {
                        var tooltip_template = '<table class="table tooltip_table">'+
                                                  '<tbody>'+
                                                      '<tr>'+
                                                          '<th scope="row">Confidence</th>'+
                                                          '<td>Confidence graph/values go here</td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                          '<th scope="row">VG Terms</th>'+
                                                          '<td><ul>'+ listify(causes) +'</ul></td>'+
                                                      '</tr>'+
                                                  '</tbody>'+
                                              '</table>';
                        origin.tooltipster('content', tooltip_template);
                    });

                    // this returned string will overwrite the content of the tooltip for the time being
                    return '<b>Loading...</b>';
                }
                else {
                    // return nothing : the initialization continues normally with its content unchanged.
                }
            },
            contentAsHTML: 'true',
            theme: 'tooltipster-shadow',
            position: 'top-right',
            minWidth: 200,
            maxWidth: 350,
            trigger: 'click',
        });

        $(this).click();
    });

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

function extractCauses(queryExplanation, col_index) {

    var optimizedQuery = queryExplanation.split(/--- Optimized Query ---/i)[1];

    var pos = 0;
    var project = /PROJECT/i.exec(optimizedQuery);
    if(project) {
        pos = project.index + "PROJECT".length;
    }

    var projectArgs = getArguments(optimizedQuery, pos, "[", "]");

    if(col_index == -1) {
        return findVGTerms(projectArgs[projectArgs.length-1][1]);
    } else {
        return findVGTerms(projectArgs[col_index-1][1]);
    }
}

function getArguments(str, pos, opening, closing) {

    var braces = 0;
    var i = pos;

    do {
        if(str.charAt(i) === opening) braces++;
        else if(str.charAt(i) === closing) braces--;
        i++;
    } while(braces > 0 && i < str.length);

    if(braces != 0) {
        throw new Error('Mis-formed arguments');
    }

    var argsString = str.substring(pos+1, i-1);
    var args = argsString.split(",");
    var argsMap = new Array(args.length);

    args.forEach(function (currentValue, index, array) {
        var arg = currentValue.split("<=");
        argsMap[index] = [arg[0].trim(), arg[1].trim()];
    });

    return argsMap;
}

function findVGTerms(str) {
    var re = /{{([^}]*)}}/img;
    var causes = [];
    var match;

    while(match = re.exec(str)) {
        causes.push(match[1]);
    }

    return causes;
}