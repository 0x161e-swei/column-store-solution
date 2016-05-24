/**
 * Created by logicRabbit on 1/21/16.
 */
var d3 = require('d3');
var jsdom = require('jsdom');
var doc = jsdom.jsdom();
var brushMap = require('./brushMap');


var getBrushMap = function (params) {

    //console.log("brushMap " + interactionMap);

    var selector = params.containerId;
    var partitionNumData = params.data;
    //console.log("data is+++++!!!!!" + partitionNumData[0].length);
    var interactionMap = brushMap().data(params.data);
    d3.select(doc.body).append('div').attr('id', params.containerId).call(interactionMap);
    var svg = d3.select(doc.getElementById(selector)).node().outerHTML;
    //console.log(svgMap);
    d3.select(doc.getElementById(selector)).remove();

    return svg;

};


module.exports = {
    getBrushMap: getBrushMap
};
