<%!from desktop.views import commonheader, commonfooter %>
<%namespace name="shared" file="shared_components.mako" />

${commonheader("variants", "variants", user) | n,unicode}
${shared.menubar(section='query')}

## Use double hashes for a mako template comment
## Main body

<div class="container-fluid">
  <div class="card">
    <h2 class="card-heading simple">Insert/Upload Data</h2>
    <div class="card-body">
        Data initialized.<br/>
    </div>
  </div>
</div>
${commonfooter(messages) | n,unicode}
