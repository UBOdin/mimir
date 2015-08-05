$( document ).ready(function() {

    /*
    Basic interactive animations
    */
    $("#about_btn").on("click", function() {
        $("#about").toggle(100);
    });

    $("#upload").on("click", function() {
        $("#drop_area").toggle(100);
    });

    $(".close_btn").on("click", function() {
        $(this).parent().hide(100);
    });

    /*
    Generate query for showing tables and lenses
    */
    $(document).on("click", ".table_link, .lens_link", function() {
        var table = $(this).html();
        var query = "SELECT * FROM "+table+";";

        $("#query_textarea").val(query);
        $("#query_btn").trigger("click");
    });

    /*
    Change the working database
    */
    $(".db_link").on("click", function() {
        var db = $(this).html();
        var curr = $("#curr_db").html().trim();

        if(db.valueOf() !== curr.valueOf()) {
            $("#db_field").val(db);
            $("#query_textarea").val("");
            $("#query_btn").trigger("click");
        }
    });

    /*
    Create a new database
    */
    $("#create_database").on("click", function() {
        var db = prompt("Please enter a name for the new database", "awesomedb");
        var existing_dbs = new Array();

        // Check for valid name
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


    /*
    Plugin Configurations
    */

    /*
    ColResizable
    http://www.bacubacu.com/colresizable/

    Resizable columns, automatic adjustment
    for equally spaced columns
    */
    $("#result_table").colResizable( {
        liveDrag: true,
        minWidth: 10
    });

    /*
    Dropzone
    http://www.dropzonejs.com

    Enables csv upload
    */
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

    /*
    Tooltipster
    http://iamceege.github.io/tooltipster/

    For tooltips
    */

    // Cell level uncertainty
    $(".non_deterministic_cell").one("click", function(e) {
        $(this).tooltipster({
            animation: 'grow',
            contentAsHTML: 'true',
            delay: 10,
            minWidth: 350,
            maxWidth: 350,
            position: 'bottom',
            theme: 'tooltipster-shadow',
            trigger: 'click',
            functionInit: function(origin, content) {

                // Get content through ajax queries

                var col_index = origin.prevAll().length;
                var col = origin.parents('table').find('th').eq(col_index).text();
                var row = origin.parent().children(".rowid_col").html();
                var query = $("#query_textarea").val().replace(";","");
                var db = $("#db_field").val();

                var bounds = [];
                var variance;
                var conf_int = "";
                var causes = [];

                var fault = false;
                var errormessage = "";

                var data_params_query = 'queryjson?query=SELECT BOUNDS('+col
                                        +'), VAR('+col+'), CONFIDENCE('+col+') FROM ('+query
                                        +') AS TEMP WHERE ROWID = '+row+';&db='+db;

                var vgterms_query = 'vgterms?query='+query+';&row='+row+'&ind='+ col_index
                                        +'&db='+db;

                if (content == null) {

                    $.when(
                        $.get(data_params_query, function (res) {
                            if(res.hasOwnProperty('error')) {
                                fault = true;
                                errormessage += res.error+'<br/>';
                            }
                            else {
                                console.log(res);
                                bounds[0] = res.data[0][1] == "NULL" ? "NULL" : parseFloat(res.data[0][1]).toFixed(2);
                                bounds[1] = res.data[0][2] == "NULL" ? "NULL" : parseFloat(res.data[0][2]).toFixed(2);
                                variance  = res.data[0][3] == "NULL" ? "NULL" : parseFloat(res.data[0][3]).toFixed(2);
                                conf_int  = res.data[0][4] == "NULL" ? "NULL" : res.data[0][4].replace(/\'/g, '');
                            }
                        }),
                        $.get(vgterms_query, function (res) {
                            if(res.hasOwnProperty('error')) {
                                fault = true;
                                errormessage += res.error+'<br/>';
                            }
                            else {
                                causes = res;
                            }
                        })
                    ).then( function() {
                        if(fault) {
                            origin.tooltipster('content', 'Something went wrong!<br/><br/>'+errormessage);
                        }
                        else {
                            var tooltip_template = '<table class="table tooltip_table">'+
                                                  '<tbody>'+
                                                      '<tr>'+
                                                          '<th scope="row">Bounds</th>'+
                                                          '<td class="number">'+bounds[0]+' | '+bounds[1]+'</td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                          '<th scope="row">Variance</th>'+
                                                          '<td class="number">'+variance+'</td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                          '<th scope="row">Confidence Interval</th>'+
                                                          '<td class="number">'+conf_int+'</td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                          '<th scope="row">Reasons</th>'+
                                                          '<td><ul>'+listify(causes)+'</ul></td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                      '<tr>'+
                                                         '<td colspan=2>'+
                                                         '<a class="ttOption approve">  Approve   </a>'+
                                                         '<a class="ttOption fix">  Fix</a>'+
                                                         '</td>'+
                                                      '</tr>'+
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
        });

        // User click only attached the listener, now trigger click
        // to actually display the tooltip
        $(this).click();
    });

    // Row level uncertainty
    $(".non_deterministic_row").one("click", function(e) {
        $(this).tooltipster({
            animation: 'grow',
            contentAsHTML: 'true',
            delay: 10,
            minWidth: 350,
            maxWidth: 350,
            position: 'top-right',
            theme: 'tooltipster-shadow',
            trigger: 'click',
            functionInit: function(origin, content) {

                var row = origin.children(".rowid_col").html();
                var query = $("#query_textarea").val().replace(";","");
                var db = $("#db_field").val();

                var prob;
                var causes = [];

                var fault = false;
                var errormessage = "";

                var prob_query = 'queryjson?query=SELECT PROB() FROM ('+query+') AS TEMP WHERE ROWID = '+row+';&db='+db;
                var vgterms_query = 'vgterms?query='+query+';&row='+row+'&ind='+-1+'&db='+db;

                if (content == null) {
                    $.when(
                        $.get(prob_query, function (res) {
                            if(res.hasOwnProperty('error')) {
                                fault = true;
                                errormessage += res.error+'<br/>';
                            }
                            else {
                                prob = res.data[0][1];
                            }
                        }),
                        $.get(vgterms_query, function (res) {
                            if(res.hasOwnProperty('error')) {
                                fault = true;
                                errormessage += res.error+'<br/>';
                            }
                            else {
                                causes = res;
                            }
                        })
                    ).then( function() {
                        if(fault) {
                            origin.tooltipster('content', 'Something went wrong!<br/><br/>'+errormessage);
                        }
                        else {
                            var tooltip_template = '<table class="table tooltip_table">'+
                                                  '<tbody>'+
                                                      '<tr>'+
                                                          '<th scope="row">Confidence</th>'+
                                                          '<td class="number">'+prob+'</td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                          '<th scope="row">Reasons</th>'+
                                                          '<td><ul>'+ listify(causes) +'</ul></td>'+
                                                      '</tr>'+
                                                      '<tr>'+
                                                         '<td colspan=2>'+
                                                         '<a class="ttOption approve">  Approve  </a>'+
                                                         '<a class="ttOption fix">  Fix</a>'+
                                                         '</td>'+
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
        });

        $(this).click();
    });
    drawGraph();
});


/*
Utility functions
*/
function listify(causes) {
    var i;
    var result = '';
    for(i = 0; i<causes.length; i++) {
        result += '<li class="paperclip">'+ causes[i] +'</li>'
    }

    return result;
}