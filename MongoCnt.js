(function () {
    var myConnector = tableau.makeConnector();

    myConnector.getSchema = function (schemaCallback) {
        var cols = [{
            id: "timestamp",
            alias:"timestamp",
            dataType: tableau.dataTypeEnum.datetime

        }, {
            id: "streamId",
            alias: "streamId",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "hygrometry",
            alias: "hygrometry",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "temperature",
            alias: "temperature",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "revmin",
            alias: "revmin",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "lat",
            alias: "lat",
            dataType: tableau.dataTypeEnum.float
        },{
            id: "lon",
            alias: "lon",
            dataType: tableau.dataTypeEnum.float
        }];

        var tableSchema = {
            id: "testAppOAB",
            alias: "1st test app OAB",
            columns: cols
        };


        schemaCallback([tableSchema]);
    };

    myConnector.getData = function(table, doneCallback) {
        $.ajaxSetup({
            headers : {
                'X-API-KEY' : 'XXXX' // put you Live Objects API key with Application role
            }
        });
        var connectionData = JSON.parse(tableau.connectionData);
        var max_iterations = connectionData.max_iterations;
        // $.getJSON("https://liveobjects.orange-business.com/api/v0/data/streams/PUT_YOUR_STREAMID_HERE?limit="+max_iterations, function(resp) {
        $.getJSON("https://liveobjects.orange-business.com/api/v0/data/streams/androidFLG35732098787059?limit="+max_iterations, function(resp) {
            var feat = resp,
                tableData = [];

            tableau.log(".getJSON");
            tableau.log(resp);
            // Iterate over the JSON object
            for (var i = 0, len = feat.length; i < len; i++) {
              var tim = moment(feat[i].timestamp).format('Y-MM-DD HH:mm:ss');
              tableData.push({
                    "timestamp": tim,
                    "streamId": feat[i].streamId,
                    "hygrometry": feat[i].value.hygrometry,
                    "temperature": feat[i].value.temperature,
                    "revmin": feat[i].value.revmin,
                    "lat": feat[i].location.lat,
                    "lon": feat[i].location.lon
                });
            }

            table.appendRows(tableData);
            doneCallback();
        });
    };

    setupConnector = function() {
       var max_iterations = $("#max_iterations").val();

       if (max_iterations >0 && max_iterations <= 1000) {
           var connectionData = {
               "max_iterations": parseInt(max_iterations)
           };
           tableau.connectionData = JSON.stringify(connectionData);
           tableau.submit();
       }else {
         alert('Enter a value between 1 and 1000')
       }
    };

    tableau.registerConnector(myConnector);

    $(document).ready(function () {
        $("#submitButton").click(function () {
            tableau.connectionName = "Live Objects OAB App Test 1";
            setupConnector();
        });
    });
    $('#inputForm').submit(function(event) {
        event.preventDefault();
        setupConnector();

    });

})();
