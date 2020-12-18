
class S6_Plugin_Progress {

  constructor( deck, options ) {

    const progressParent = document.createElement('div'),
          progressBar    = document.createElement('div');

    progressParent.className = 'bespoke-progress-parent';
    progressBar.className    = 'bespoke-progress-bar';
    progressParent.prepend( progressBar );
    deck.parent.prepend( progressParent );

    deck.on( 'activate', ev =>
      progressBar.style.width = (ev.index * 100 / (deck.slides.length - 1)) + '%'
    );
  }
} // class S6_Plugin_Progress



//////////////////////////////
// add global S6 "export"
//   e.g. lets you call progress( options ) for plugins array config

var S6 = S6 || {};
S6.progress = options => deck => new S6_Plugin_Progress( deck, options );
