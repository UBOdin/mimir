function node(){
	this.children = [];
	this.name = "";
	this.position = {};
}

node.prototype.size = function(){
	var sum = 1;
	for (var i = this.children.length - 1; i >= 0; i--) {
		sum += this.children[i].size();
	}
	return sum;
};

node.prototype.links = function(){
	var self = this;
	if(self.children.length == 0)
		return [];
	var arr = [{source: self.position, target: self.children[0].position}].concat(self.children[0].links());
	if(self.children.length > 1)
		arr = arr.concat([{source: self.position, target: self.children[1].position}]).concat(self.children[1].links());
	return arr;
}

node.prototype.getAllPositions = function(){
	var arr = [this.position]
	switch(this.children.length) {
		case 1: arr = arr.concat(this.children[0].getAllPositions());
				break;
		case 2: arr = arr.concat(this.children[0].getAllPositions())
						.concat(this.children[1].getAllPositions());
				break;
		default: break;
	}
	return arr;
}

function propogatePositions(node, xVal, yVal, div){
	var XDIFF = 100, YDIFF = 80;
	node.position = {x: xVal, y: yVal, name: node.name};
	if(node.children.length == 0)
		return node;
	else if(node.children.length == 1)
		node.children = [propogatePositions(node.children[0], xVal - XDIFF, yVal, div)];
	else {
		node.children = [propogatePositions(node.children[0], xVal - XDIFF, yVal + YDIFF/(2*div), 2*div),
						 propogatePositions(node.children[1], xVal - XDIFF, yVal - YDIFF/(2*div), 2*div)];
	}
	return node;
}

function createTree(flow){
    var root = new node();
    root.name = flow.name;
    var rootChild = [];
    for(var i = 0; i < flow.children.length; i++){
        rootChild.push(createTree(flow.children[i]));
    }
    root.children = rootChild;
    return root;
}

function graph(node){
	var width = 700,
    height = 300;
	var radius = 5;
	node = propogatePositions(node, width-10, height/2, 1);
	var nodes = node.getAllPositions();
	var links = node.links();

	var vis = d3.select("#graph")
	  .append("svg:svg")
	  .attr("class", "stage")
	  .attr("width", width)
	  .attr("height", height);

	var uinodes = vis.selectAll("circle.node")
	  .data(nodes)
	  .enter().append("g")
	  .attr("class", "node");

	uinodes.append("svg:circle")
	  .attr("cx", function(d) {return d.x;})
	  .attr("cy", function(d) {return d.y;})
	  .attr("r", radius)
	  .attr("fill", "black");

	uinodes.append("text")
	  .text(function(d, i) {return d.name;})
	  .attr("x", function(d, i) {return d.x;})
	  .attr("y", function(d, i) {return d.y-10;})
	  .attr("font-family", "Bree Serif")
	  .attr("fill", "black")
	  .attr("font-size", "1em");

	vis.selectAll(".line")
	   .data(links)
	   .enter()
	   .append("line")
	   .attr("x1", function(d) { return d.source.x })
	   .attr("y1", function(d) { return d.source.y })
	   .attr("x2", function(d) { return d.target.x })
	   .attr("y2", function(d) { return d.target.y })
	   .style("stroke", "rgb(6,120,155)");
}

function drawGraph(){
    var flow = $('#flow').val();
    if(flow == undefined)
        return;
	var root = new node();
	root.name = "result";
	root.children = [createTree($.parseJSON(flow))];
	graph(root);
}