 

$("#submit_stock").click(function(){
	console.log($("#stock_symbol").val());
	$.ajax({
		url:"http://localhost:8080/addStockData?stock_symbol="+$("#stock_symbol").val(),
		type:"POST",
		success: function(data){
			console.log("success send data");
		}
	});
});

$(function(){
	var socket = io();
    // Data
    var smoothie = new SmoothieChart();
    smoothie.streamTo(document.getElementById("mycanvas"));
    var line1 = new TimeSeries();
    var line2 = new TimeSeries();

    // Add to SmoothieChart
    smoothie.addTimeSeries(line1);
    smoothie.addTimeSeries(line2);
	socket.on('data',function(message){
		var json = $.parseJSON(message);
        var stock_symbol = json[0].StockSymbol;
		var price = json[0].LastTradePrice;
        line1.append(new Date().getTime(), price);	 
	});
});