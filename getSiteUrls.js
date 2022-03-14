//paste this into chrome console

var urls = document.getElementsByTagName('a');
for (var i = 0; i < urls.length; i++) {
    console.log ( urls[i].getAttribute('href') );
}
