$(document).ready(function() {
    //Not used anymore, but we let the code as it may be handful another time
  $("#insertForm").submit(function(e){
    e.preventDefault(e);
    
    var import_file = document.getElementById("import_file").value;
    var samples_ids = document.getElementById("samples_ids").value;
     
    var params = 'import_file='+encodeURI(import_file)+ '&samples_ids='+encodeURI(samples_ids);
    if (import_file == '' || samples_ids == '') {
      $("#result").text('Please, fill all the fields');
      $("#result").removeClass("bad-info");
      $("#result").removeClass("great-info");
      $("#result").addClass("bad-info");
    } else {
      var $form = $("#insertForm"), url = $form.attr( "action" );
     
     //Disabling the form
     $('#insertFormSubmit').attr('disabled', 'disabled');
     $('#import_file').attr('disabled', 'disabled');
     $('#samples_ids').attr('disabled', 'disabled');
     
     //Say to the user to wait
      $("#result").text("Please wait...");
      $("#result").removeClass("bad-info");
      $("#result").removeClass("great-info");
      $("#result").addClass("great-info");

     //Sending the request
     $.ajax({
            type: 'POST',
            url: '/variants/api/general/insert/?user.name=cloudera',
            data: params,        
            dataType: 'html',
            contentType: "application/json",    
            success: function(response) {
              obj = $.parseJSON(response);
              
              if(typeof obj.status !== 'undefined' && obj.status == 1) {
                $("#result").text(String(obj.data));
                $("#result").removeClass("bad-info");
                $("#result").removeClass("great-info");
                $("#result").addClass("great-info");
              } else {
                $("#result").text(obj.data);
                $("#result").removeClass("bad-info");
                $("#result").removeClass("great-info");
                $("#result").addClass("bad-info");   
              }
            }, 
            complete: function(jqXHR) {
              //Values of the form to zero
             $('#samples_ids').val('');
              
             //Enabling the form again the form
             $('#insertFormSubmit').removeAttr('disabled');
             $('#import_file').removeAttr('disabled');
             $('#samples_ids').removeAttr('disabled');
            },
            error: function(jqXHR) {
              $("#result").text("Sorry, an error occurred.");
              $("#result").removeClass("bad-info");
              $("#result").removeClass("great-info");
              $("#result").addClass("bad-info");      
            } 
        });        
    }
    return false;
  });
});
