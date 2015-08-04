<%!from desktop.views import commonheader, commonfooter %>
<%namespace name="shared" file="shared_components.mako" />

<script src="/static/variants/js/jquery-1.11.1.min.js"></script>

<script src="/static/variants/js/handsontable.full.js"></script>
<script src="/static/variants/js/moment.js"></script>
<script src="/static/variants/js/pikaday.js"></script>
<link rel="stylesheet" media="screen" href="/static/variants/css/handsontable.full.css">
<link rel="stylesheet" media="screen" href="/static/variants/css/pikaday.css">
<link rel="stylesheet" media="screen" href="/static/variants/css/samples.css">

<script><!-- -->
$(document).ready(function () {
    var data = [
            % if not error_sample and not error_get:
                % for key, value in enumerate(samples):
                    ['${value}'],
                % endfor
            % endif
            ];
    var container = document.getElementById('variants-info'),
    hot;

    hot = new Handsontable(container, {
        data: data,
        minSpareRows: 1,
        fixedColumnsLeft: 1,
        fixedRowsTop: 0,
        maxRows: ${samples_quantity},
        maxCols: 10,
        colHeaders: [
        % if q:
            % for field in q:
                % if field == "main_title":
                    ['${questions["sample_registration"][field]}'],
                % else:
                    ['${questions["sample_registration"][field]['question']}'],
                % endif
            % endfor %
        % endif
                ],
        colWidths: [100, 145, 100, 100, 100, 150, 150, 145, 200, 175],
        contextMenu: true,
        manualColumnResize: true,
        manualRowResize: true,
        columns: [
        % if q:
                % for field in q:
                    % if field == "main_title":
                        {strict: true,allowInvalid: false},
                    % else:
                        % if questions["sample_registration"][field]["field"] == "text":
                            {strict: true},
                        % elif questions["sample_registration"][field]["field"] == "select":
                            {type: 'autocomplete', source: [
                                    % for subfield in questions["sample_registration"][field]["fields"]:
                                        '${subfield}',
                                    % endfor
                               ], strict: true, allowInvalid: false},
                        % elif questions["sample_registration"][field]["field"] == "date":
                            {type: 'date', dateFormat: 'MM/DD/YYYY', correctFormat: true, strict: true,allowInvalid: false},
                        % endif
                    % endif
                % endfor %
        % endif
        ],
        cells: function (row, col, prop) {
            var cellProperties = {};

            if (col === 0 || this.instance.getData()[row][col] === 'readOnly') {
              cellProperties.readOnly = true; // make cell read-only if it is first row or the text reads 'readOnly'
            }

            return cellProperties;
        }
    });


    $("#handson-form").submit(function(e){

        //The simple getData remove empty cell, which is not very practical...
        //Each line will be separated by a ';' and each cell by a ','
        var completeData = "";
        var currentCellData = "";
        for(var x=0; x < hot.countRows(); x++) {
            for(var y=0; y < hot.countCols(); y++) {
                if(y > 0)
                    completeData += ",";
                currentCellData = hot.getDataAtCell(x, y)

                if(currentCellData !== undefined)
                    completeData += currentCellData;
            }
            completeData += ";"
        }

        $('#vcf_data').val(completeData);
    });


    });
</script>


${commonheader("variants", "variants", user) | n,unicode}
${shared.menubar(section='sample')}

<link rel="stylesheet" href="/static/variants/css/variants.css">
<script src="/static/variants/js/variants.js"></script>

## Use double hashes for a mako template comment
## Main body

<div class="container-fluid">
    <div class="card">
        <h2 class="card-heading simple">
            %if filename:
                Adding information for the samples in the file "${filename}"</h2>
            % else:
                Adding information to samples
            %endif
        <div class="card-body variants">
            <div class="great-info" id="result"></div><br/><br/>

            <div class="insert-samples">
                <form action="" method="POST" name="insert-form" id="handson-form">
                % if error_get:
                    <strong><font color="red">You have to give a vcf file!</font></strong>
                % elif error_sample:
                    <strong><font color="red">We have found no sample information in the vcf. <br/>The file may be corrupted or the format not taken into account in the current version of the code.</font></strong>
                % else:

                    <!-- If we already got the form, we display the result-->
                    % if result and result['status'] == 1:
                            <strong><font color="green">Data correctly added.</font></strong>
                        <br/>
                        <a href="/variants/sample/index/interface/">Go back to the main page</a>
                    % else:
                        % if result:
                            <strong><font color="red">${result['error']}</font></strong><br/>
                        % endif

                            <br/>
                        <div style="display:inline-block;max-width:95%;min-height:200px;max-height:500px;overflow: scroll;">
                            <div id="variants-info" class="handsontable"></div>
                       </div>
                        <br/><br/>
                        <input type="text" value="" id="vcf_data" name="vcf_data" style="display:none"/>
                        <input type="submit" value="Import" id="save-handson"/>
                        <br/>
                    % endif
                </form>
                % endif
            </div>
        </div>
    </div>
</div>
${commonfooter(messages) | n,unicode}
