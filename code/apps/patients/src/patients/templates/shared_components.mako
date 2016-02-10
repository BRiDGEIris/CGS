
<%!
def is_selected(section, matcher):
  if section == matcher:
    return "active"
  else:
    return ""
%>

<%def name="menubar(section='')">
  <div class="navbar navbar-inverse navbar-fixed-top nokids">
    <div class="navbar-inner">
      <div class="container-fluid">
        <div class="nav-collapse">
          <ul class="nav">
            <li class="currentApp">
              <a href="/patients">
                <img src="/patients/static/art/icon_genomicAPI_48.png" class="app-icon" />
                CGS - Patients
              </a>
             </li>
             <li class="${is_selected(section, 'index')}"><a href="/patients/">Index</a></li>
             <li class="${is_selected(section, 'sample')}"><a href="/patients/sample/index/interface/">Import patient</a></li>
          </ul>
        </div>
      </div>
    </div>
  </div>
</%def>
