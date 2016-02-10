
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
              <a href="/variants">
                <img src="/variants/static/art/icon_genomicAPI_48.png" class="app-icon" />
                CGS - Variants
              </a>
             </li>
             <li class="${is_selected(section, 'index')}"><a href="/variants/">Index</a></li>
             <li class="${is_selected(section, 'query')}"><a href="/variants/query/index/interface/">Query</a></li>
             <li class="${is_selected(section, 'sample')}"><a href="/variants/sample/index/interface/">Sample</a></li>
          </ul>
        </div>
      </div>
    </div>
  </div>
</%def>
