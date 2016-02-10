<%!from desktop.views import commonheader, commonfooter %>
<%namespace name="shared" file="shared_components.mako" />

${commonheader("variants", "variants", user) | n,unicode}
${shared.menubar(section='sample')}

<link rel="stylesheet" href="/static/variants/css/variants.css">

## Use double hashes for a mako template comment
## Main body

<div class="container-fluid">
  <div class="card">
    <h2 class="card-heading simple">Manage the samples</h2>
    <div class="card-body genomicAPI">

        ## Some general information how to upload sample data
        <h4>Howto: Upload your data</h4>
        <p>
            <h5>1. Go to your hdfs directoy with 'File Browser' (top-right corner of any webpage)</h5>
            <h5>2. Upload your .vcf, .bam, .fastq and other files with the upload button on the right part of the page.</h5>
            <h5>3. Go back to this page, select one .vcf file previously uploaded and submit the form.</h5>
            <h5>4. Insert the information relevant for each sample automatically found in the file.</h5>
            <h5>5. The data are now present in the cgs file system! </h5>
        </p>
        <br/>

        ## The form needed to go to sample/insert/interface with the right file selected
        <h4>Add data to your samples files</h4>
        % if total_files > 0:
            Files found in your directory:<br/>
            <form action="/variants/sample/insert/interface/" method="GET">
                <select name="vcf">
                    % for value in files:
                        <option value="${value}" selected>${value}</option>
                    % endfor
                </select><br/>
                <input type="submit" name="type" value="Add data to the samples" /><br/>
                <input type="submit" name="type" value="Import directly" /><br/>
            </form>
        % else:
            No .vcf file were found in your directory.
        % endif
        <br/><br/>
    </div>
  </div>
</div>
<script src="/static/variants/js/variants.js"></script>
${commonfooter(messages) | n,unicode}
