<%!from desktop.views import commonheader, commonfooter %>
<%namespace name="shared" file="shared_components.mako" />

${commonheader("GEMAN", "GEMAN", user) | n,unicode}
${shared.menubar(section='query')}

<link rel="stylesheet" href="/GEMAN/static/css/GEMAN.css">

## Use double hashes for a mako template comment
## Main body

<div class="container-fluid">
  <div class="card">
    <h2 class="card-heading simple">Query your data</h2>
    <div class="card-body genomicAPI">
      <form action="" method="POST" class="queryForm">
        <br/>
        <div class="left-box">
        <label for="query_type">Type of query:</label>
        </div>
        <div class="right-box">
          <select name="query_type">
            <option value="0" selected>Genomic API (SQL-Like)</option>
            <option value="1">Google Genomics</option>
            <option value="2">HBase</option>
            <option value="3">Hive</option>
          </select><br/><br/>
        </div>
        <br/>
        <div class="left-box">
          <label for="query">Query:</label>
        </div>
        <div class="right-box">
          <textarea rows="5" name="query" cols="100" maxlength="500"></textarea>
          <br/><br/>
        </div>
        <br/>
        <input type="submit" value="Execute" />
        <br/>
      </form>
    
    </div>
  </div>
</div>
<link rel="stylesheet" href="/GEMAN/static/js/GEMAN.js">
${commonfooter(messages) | n,unicode}
