
function load_graph(uri, elm_id, page_len) {
    const request = new Request(uri);
    fetch(request)
	.then(response => response.json())
	.then(resp => plot(resp, elm_id, page_len))
}

function plot(resp, elm_id, page_len) {
    var el = document.getElementById(elm_id);
    resp.options.width = el.clientWidth;
    let uplot = new uPlot(resp.options, resp.data, document.getElementById(elm_id));
}
