$(function() {
    var editor = ace.edit("editor");
    editor.session.setMode("ace/mode/json");
    editor.setTheme("ace/theme/monokai");
    editor.setOptions({
        fontSize: "15pt"
    });
    var errorArea = $("#error");
    errorArea.hide();

    $("#submit").on("click", function() {
        $.ajax({
            type: "POST",
            url: window.location.href,
            data: JSON.stringify({
                message: JSON.parse(editor.getValue())
            }),
            contentType: "application/json",
            dataType: "json"
        }).done(function(response) {
            editor.setValue("");
            errorArea.text("");
            errorArea.hide();
        }).fail(function(response) {
            errorArea.text(response["responseJSON"]["description"]);
            errorArea.show();
        });
    });
});
