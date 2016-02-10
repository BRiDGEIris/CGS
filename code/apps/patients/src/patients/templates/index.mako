<%!from desktop.views import commonheader, commonfooter %>
<%namespace name="shared" file="shared_components.mako" />

${commonheader("patients", "patients", user) | n,unicode}
${shared.menubar(section='index')}

## Use double hashes for a mako template comment
## Main body

<div class="container-fluid">
  <div class="card">
    <h2 class="card-heading simple">Welcome to CGS - patients!</h2>
    <div class="card-body">
      <p>Hello!</p>
    </div>
  </div>
</div>
${commonfooter(messages) | n,unicode}
