(function () {
    var myConnector = tableau.makeConnector();

    myConnector.getSchema = function (schemaCallback) {
        var cols = [{
            id: "timestamp",
            alias: "timestamp",
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
        }, {
            id: "lon",
            alias: "lon",
            dataType: tableau.dataTypeEnum.float
        }];

        var tableSchema = {
            id: "testAppOAB",
            alias: "1er test app OAB",
            columns: cols
        };


        schemaCallback([tableSchema]);
    };

    myConnector.getNextData = function (table, doneCallback, firstElementIndex, loadedCount, nextElementId) {
        $.ajaxSetup({
            headers: {
                'X-API-KEY': 'XXXX' // put you Live Objects API key with Application role
            }
        });
        var connectionData = JSON.parse(tableau.connectionData);
        var max_iterations = connectionData.max_iterations;
        var stream_id = connectionData.stream_id;
        var limit = Math.min(max_iterations, 1000);

        // For less than 1000 raws : $.getJSON("https://liveobjects.orange-business.com/api/v0/data/streams/PUT_YOUR_STREAMID_HERE?limit="+max_iterations, function(resp) {
        // For more than 1000 raws use the Id of the last raw from the last request : $.getJSON("https://liveobjects.orange-business.com/api/v0/data/streams/PUT_YOUR_STREAMID_HERE?limit="+max_iterations+ "&bookmarkId=" + nextElementId, function(resp) {

        // Build the request
        var get = "https://liveobjects.orange-business.com/api/v0/data/streams/"+stream_id+"?limit="
            + limit;

        if (0 < firstElementIndex) {
            console.log('Will look for more ' + limit + ' messages');
            get += "&bookmarkId=" + nextElementId;
        } else {
            console.log('Will look for ' + limit + ' messages');
        }

        $.getJSON(get, function (resp) {
            var feat = resp,
                tableData = [],
                lastId = "";

            tableau.log(".getJSON");
            tableau.log(resp);
            // Iterate over the JSON object
            for (var i = 0, len = feat.length; i < len; i++) {
                var tim = moment(feat[i].timestamp).format('Y-MM-DD HH:mm:ss');
                lastId = feat[i].id;
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
            loadedCount += feat.length;
            firstElementIndex += limit;

            console.log('found ' + loadedCount + ' messages.')

            if (max_iterations > 1000 && loadedCount < max_iterations && firstElementIndex === loadedCount) {
                myConnector.getNextData(table, doneCallback, firstElementIndex, loadedCount, lastId);
            } else {
                doneCallback();
            }

        });
    }

    myConnector.getData = function (table, doneCallback) {
        myConnector.getNextData(table, doneCallback, 0, 0, null);
    };

    setupConnector = function () {
        var max_iterations = $("#max_iterations").val();
        var stream_id = $("#stream_id").val();

        if (max_iterations > 0) {
            var connectionData = {
                "max_iterations": parseInt(max_iterations),
                "stream_id": stream_id
            };
            tableau.connectionData = JSON.stringify(connectionData);
            tableau.submit();
        } else {
            alert('Saisissez une valeur supérieur à 1')
        }
    };

    tableau.registerConnector(myConnector);

    $(document).ready(function () {
        $("#submitButton").click(function () {
            tableau.connectionName = "Live Objects OAB App Test 1";
            setupConnector();
        });
    });
    $('#inputForm').submit(function (event) {
        event.preventDefault();
        setupConnector();

    });

})();
