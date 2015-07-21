if (window.console) {
    console.log("Welcome to your Play application's JavaScript!");
}

$( document ).ready(function() {

    $(".table_link, .lens_link").on("click", function() {
        var table = $(this).html();
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

                    var fault = false;

                    var data_params_query = 'queryjson?query=SELECT BOUNDS('+col
                                            +'), VAR('+col+'), CONFIDENCE('+col+') FROM ('+query
                                            +') AS TEMP WHERE ROWID = '+row+';&db='+db;

                    var vgterms_query = 'vgterms?query='+query+';&ind='+ (col_index-1)   // because of ROW_ID
                                            +'&db='+db;

                    if (content == null) {

                        $.when(
                            $.get(data_params_query, function (res) {
                                if(res.hasOwnProperty('result') && res.result.startsWith("Command Ignored")) {
                                    fault = true;
                                }
                                else {
                                    bounds[0] = res.data[0][1];
                                    bounds[1] = res.data[0][2];
                                    variance = res.data[0][3];
                                    var confStr = res.data[0][4];
                                    var confVal = confStr.split(" - ");
                                    conf_int[0] = confVal[0].substring(1);
                                    conf_int[1] = confVal[1].substring(0, confVal[1].length-1);
                                }
                            }),
                            $.get(vgterms_query, function (res) {
                                if(res.hasOwnProperty('result') && res.result.startsWith("Command Ignored")) {
                                    fault = true;
                                }
                                else {
                                    causes = res;
                                }
                            })
                        ).then( function() {
                            if(fault) {
                                origin.tooltipster('content', 'Something went wrong!');
                            }
                            else {
                                var tooltip_template = '<table class="table tooltip_table">'+
                                                      '<tbody>'+
                                                          '<tr>'+
                                                              '<th scope="row">Bounds</th>'+
                                                              '<td class="number">'+ parseFloat(bounds[0]).toFixed(2) +' - '+ parseFloat(bounds[1]).toFixed(2) +'</td>'+
                                                          '</tr>'+
                                                          '<tr>'+
                                                              '<th scope="row">Variance</th>'+
                                                              '<td class="number">'+ parseFloat(variance).toFixed(2) +'</td>'+
                                                          '</tr>'+
                                                          '<tr>'+
                                                              '<th scope="row">Confidence Interval</th>'+
                                                              '<td class="number">'+ parseFloat(conf_int[0]).toFixed(2) + ' - '+ parseFloat(conf_int[1]).toFixed(2) +'</td>'+
                                                          '</tr>'+
                                                          '<tr>'+
                                                              '<th scope="row">VG Terms</th>'+
                                                              '<td><ul>'+ listify(causes) +'</ul></td>'+
                                                          '</tr>'+
                                                      '</tbody>'+
                                                  '</table>';
                                origin.tooltipster('content', tooltip_template);
                            }
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

                var row = origin.children(".rowid_col").html();
                var query = $("#query_textarea").val().replace(";","");
                var db = $("#db_field").val();

                var prob;
                var causes = [];

                var fault = false;

                var prob_query = 'queryjson?query=SELECT PROB() FROM ('+query+') AS TEMP WHERE ROWID = '+row+';&db='+db;
                var vgterms_query = 'vgterms?query='+query+';&ind='+-1+'&db='+db;

                if (content == null) {
                    $.when(
                        $.get(prob_query, function (res) {
                            if(res.hasOwnProperty('result') && res.result.startsWith("Command Ignored")) {
                                fault = true;
                            }
                            else {
                                prob = res.data[0][1];
                            }
                        }),
                        $.get(vgterms_query, function (res) {
                            if(res.hasOwnProperty('result') && res.result.startsWith("Command Ignored")) {
                                fault = true;
                            }
                            else {
                                causes = res;
                            }
                        })
                    ).then( function() {
                        if(fault) {
                            origin.tooltipster('content', 'Something went wrong!');
                        }
                        else {
                            var tooltip_template = '<table class="table tooltip_table">'+
                                                  '<tbody>'+
                                                      '<tr>'+
                                                          '<th scope="row">Confidence</th>'+
                                                          '<td class="number">'+prob+'</td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                          '<th scope="row">VG Terms</th>'+
                                                          '<td><ul>'+ listify(causes) +'</ul></td>'+
                                                      '</tr>'+
                                                  '</tbody>'+
                                              '</table>';
                            origin.tooltipster('content', tooltip_template);
                        }
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
            minWidth: 350,
            maxWidth: 350,
            trigger: 'click',
        });

        $(this).click();
    });

    Dropzone.options.myAwesomeDropzone = {
      maxFilesize: 100000, // MB
      acceptedFiles: ".csv",
      addRemoveLinks: true,
      init: function() {
        this.on('success', function () {
            var acceptedFiles = [];
            this.getAcceptedFiles().forEach(function (element, index, array) {
                acceptedFiles.push(element.name.replace(/\.csv/i, ""));
            });
            var listedFiles = [];
            $(".table_link").each( function() {
                listedFiles.push( $(this).html() );
            });
            acceptedFiles.forEach(function (element, index, array) {
                var i;
                var found = false;
                for(i= 0; i<listedFiles.length; i++) {
                    if(element.toUpperCase() === listedFiles[i].toUpperCase()) {
                        found = true;
                        break;
                    }
                }

                if(!found) {
                    $("#tables_header").after('<li><a class="table_link ">'+element+'</a></li>');
                }
            });
        });
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