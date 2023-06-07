import numpy as np
import pandas


def calc_mape(actual, predic):
    if any(np.array(actual) < 1.0):
        actual += 0.001
    # print(actual)
    res = np.mean(np.abs((actual - predic) / actual))
    if res == np.inf:
        return -1

    return np.mean(np.abs((actual - predic) / actual))


def run_correlation(prefix, queryThreshold, cpu_MAPE_threshold, io_threshold, bandwidth_threshold,
                    bandwidth_MAPE_threshold, DEBUG=False):
    status = pandas.read_csv(prefix + "/results.csv",
                             names=['row', 'timestamp', 'status'])

    # Get the timeseries from the csv files
    server_system_io_in = pandas.read_csv(
        prefix + "/server_system.io_in.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    server_system_io_out = pandas.read_csv(
        prefix + "/server_system.io_out.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    client_net_eth0_received = pandas.read_csv(
        prefix + "/client_net.eth0_received.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    client_net_eth0_sent = pandas.read_csv(
        prefix + "/client_net.eth0_sent.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    client_system_cpu_user = pandas.read_csv(
        prefix + "/client_system.cpu_user.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    server_mysql_local_net_out = pandas.read_csv(
        prefix + "/server_mysql_local.net_out.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    server_mysql_local_queries_queries = pandas.read_csv(
        prefix + "/server_mysql_local.queries_queries.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    server_system_cpu_user = pandas.read_csv(
        prefix + "/server_system.cpu_user.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    server_mysql_local_net_received = pandas.read_csv(
        prefix + "/server_mysql_local.net_in.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    server_mysql_local_table_locks_immediate = pandas.read_csv(
        prefix + "/server_mysql_local.table_locks_immediate.csv",
        names=['row', 'timestamp', 'data']).reset_index(drop=True)

    # iterator for the datasets
    i = 1
    # Step of the dataset iteration
    offset = 9
    # A step to help with latency between querying and receiving the data
    bandwidthOffset = offset 

    # A MECHANISM FOR THE GRADING LOGIC TO ENSURE THAT IT DOESN'T SKIP ANY STATUS
    table = {}
    counter = 0
    for index, row in status.iterrows():

        if row.status == 'F':
            counter += 1
            table[index] = counter
        else:
            table[index] = 0

    # GRADING
    statusIndex = 0
    falsePositive = 0
    trueNegative = 0
    falseNegative = 0
    activeFailCounter = 0

    # Align i to where our experiment started
    while not client_system_cpu_user.timestamp[i] < \
            int(np.array(status.timestamp[statusIndex]).astype(float)) < \
            client_system_cpu_user.timestamp[i + offset]:
        i += 1

    # Iteration Flag
    flag = True

    while flag == True:
        score = 0
        bug = 0

        if DEBUG:
            print(i, statusIndex)

        try:
            if int(np.array(status.timestamp[statusIndex]).astype(float)) < client_system_cpu_user.timestamp[i]:
                statusIndex += 1

                if DEBUG:
                    print("Status is behind, moving", statusIndex, int(
                        np.array(status.timestamp[statusIndex]).astype(float)))

            if client_system_cpu_user.timestamp[i] <= \
                    int(np.array(status.timestamp[statusIndex]).astype(float)) <= \
                    client_system_cpu_user.timestamp[i + offset]:
                if DEBUG:
                    print(client_system_cpu_user.timestamp[i],
                          "\n <=", int(
                              np.array(status.timestamp[statusIndex]).astype(float)),
                          "<\n", client_system_cpu_user.timestamp[i + offset],
                          "\n")
                    print("FOUND STATUS WITHIN TIMEFRAME, GENERATING GRADING")

                if status.status[statusIndex] == "F":
                    if DEBUG:
                        print("\n\nFAIL------------------------", )
                    # If the results file indicates that we have a fail inside the timeframe we are about to
                    # inspect, then increase the counters and raise the proper flags
                    activeFailCounter += 1
                    bug = 1
                    if activeFailCounter != table[statusIndex]:
                        # The script should never reach here, if it does,
                        # it means that probably the step of the algorithm is so large
                        # that it has two statuses in one time frame, which means that it skips the labels.
                        return "STATUS SKIPPED THIS IS NOT ALLOWED"
                else:
                    # Else the results file in this index has an "S" meaning success, so just increase the index
                    # and set the bug flag to 0
                    bug = 0

        # If we encounter an exception we just pass, in this workflow it shouldn't be too important,
        # it normally should be reached upon termination of the script
        except Exception as e:
            # print("EXCEPTION", e)
            pass

        # If we reach the end of the available labels, terminate the algorithm
        if statusIndex == len(status):

            flag = False

        if DEBUG:
            print("FAILS:", activeFailCounter, "index:", statusIndex, table[statusIndex], client_system_cpu_user.timestamp[i],
                  int(np.array(status.timestamp[statusIndex]).astype(float)),
                  client_system_cpu_user.timestamp[i + offset], table[statusIndex], "BUG: ", bug, status.status[statusIndex])

        # If there are nan values, there is not much that can be done about them, just get the true label and move on
        if (
                np.isnan(np.sum(np.array(server_system_io_out.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(server_system_io_in.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(client_net_eth0_received.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(client_net_eth0_sent.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(client_system_cpu_user.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(server_mysql_local_net_out.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(server_mysql_local_queries_queries.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(server_system_cpu_user.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(server_mysql_local_net_received.data[i:i + offset]))) or
                np.isnan(np.sum(np.array(server_mysql_local_table_locks_immediate.data[i:i + offset])))):

            if DEBUG:
                print("moving on, nan")

            if bug == 1:
                score = 45
            elif bug == 0:
                score = 0

        else:
            if DEBUG:
                print(
                    "QUERIES", server_mysql_local_queries_queries.data[i:i + bandwidthOffset])

            # CORRELATION ---------------------------

            # Store the time frame's queries in a variable
            m = np.array(
                server_mysql_local_queries_queries.data[i:i + bandwidthOffset])

            # ADJUST THIS ACCORDING TO THE IDLE QUERIES OF THE DB SERVER
            # if any of the values indicates a query:
            if any(m >= queryThreshold):

                res = calc_mape(np.array(client_system_cpu_user.data[i:i + offset]),
                                np.array(server_system_cpu_user.data[i:i + offset]))

                if DEBUG:
                    print(np.array(server_system_cpu_user.data[i:i + offset]),
                          np.array(client_system_cpu_user.data[i:i + offset]))
                    print("CPU RESULT:", res, "BUG?", bug,
                          client_system_cpu_user.timestamp[i])

                # If they are similar, then add to the score
                if res < cpu_MAPE_threshold:
                    score += 10

                    if DEBUG:
                        print("plus 10 to score------------------")

                # Then check for table locks
                if all(server_mysql_local_table_locks_immediate.data[i: i + bandwidthOffset] == 0):
                    score += 10

                    if DEBUG:
                        print("LOCKS, +10")

                # After that check for I/O
                if (all(server_system_io_in.data[i: i + bandwidthOffset] < io_threshold) or
                        all(server_system_io_out.data[i: i + bandwidthOffset] > -io_threshold)):
                    score += 10

                    if DEBUG:
                        print("NO I/O, +10")

                # Finally do a check for the bandwidth between the Client and the Server node
                bandwidth_res = calc_mape(
                    np.multiply(
                        np.array(server_mysql_local_net_out.data[i:i + bandwidthOffset]), -1),
                    np.array(server_mysql_local_net_received.data[i:i + bandwidthOffset]))

                if DEBUG:
                    print(bandwidth_res)

                if bandwidth_res < bandwidth_MAPE_threshold:
                    score += 10

                    if DEBUG:
                        print("BANDWIDTHS ARE SIMILAR +10")

                    if (all(client_net_eth0_received.data[i: i + bandwidthOffset] < bandwidth_threshold) and
                        all(server_mysql_local_net_out.data[i: i + bandwidthOffset] > -bandwidth_threshold)) \
                            and \
                            (all(client_net_eth0_sent.data[i: i + bandwidthOffset] > -bandwidth_threshold) and
                             all(server_mysql_local_net_received.data[i: i + bandwidthOffset] < bandwidth_threshold)):
                        score += 10

                        if DEBUG:
                            print("DBIN or OUT ROUTE")

                if DEBUG:
                    print(
                        "LOCKS", server_mysql_local_table_locks_immediate.data[i: i + bandwidthOffset])
                    print(
                        "DBIN", server_mysql_local_net_received.data[i:i + bandwidthOffset])
                    print("CLIENTOUT",
                          client_net_eth0_sent.data[i:i + bandwidthOffset])
                    print("BANDWIDTH",
                          server_mysql_local_net_out.data[i:i + bandwidthOffset])
                    print("RECEIVED",
                          client_net_eth0_received.data[i:i + bandwidthOffset])

        if DEBUG:
            print("SCORE", score)

        # If there is an anomaly, whe got to check whether it is correct or not, for grading purposes
        if score >= 35:
            answer = 1
        else:
            answer = 0

        if bug == 1:
            if answer == 1:
                trueNegative += 1
            else:
                falsePositive += 1

        elif bug == 0:
            if answer == 1:
                falseNegative += 1

        i += offset+1

        if DEBUG:
            print(score)

    # Final results
    truePositive = len(status) - falsePositive - \
        trueNegative - falseNegative

    precision = (truePositive) / (truePositive + falsePositive)

    recall = truePositive / (truePositive + falseNegative)

    f1 = 2 * truePositive / (2*truePositive + falsePositive + falseNegative)

    answerString = "RESULTS FOR DATASET: {5}\n" \
        "{0} {2}\n" \
        "{1} {3}\n" \
        "True Positive (0 is 0) {0}\n" \
        "False Positive (1 predicted as 0, It was an anomaly, not a normal event) {2}\n" \
        "False Negative (0 predicted as 1, Not an anomaly in reality {1}\n" \
        "True Negative (1 is 1) {3}\n" \
        "Precision: {4}\n" \
        "Recall: {6}\n" \
        "F1-score: {7}\n\n".format(truePositive, falseNegative, falsePositive, trueNegative, precision,
                                   prefix, recall, f1)

    return answerString


print(run_correlation(prefix="validation",
                      queryThreshold=5,
                      cpu_MAPE_threshold=1.6,
                      io_threshold=300,
                      bandwidth_threshold=300,
                      bandwidth_MAPE_threshold=1
                      ))
print(run_correlation(prefix="validation2",
                      queryThreshold=4.3,
                      cpu_MAPE_threshold=1.6,
                      io_threshold=300,
                      bandwidth_threshold=300,
                      bandwidth_MAPE_threshold=1
                      ))
print(run_correlation(prefix="validation3",
                      queryThreshold=4.3,
                      cpu_MAPE_threshold=1.6,
                      io_threshold=300,
                      bandwidth_threshold=300,
                      bandwidth_MAPE_threshold=1
                      ))
print(run_correlation(prefix="validation4",
                      queryThreshold=4.5,
                      cpu_MAPE_threshold=1.6,
                      io_threshold=300,
                      bandwidth_threshold=300,
                      bandwidth_MAPE_threshold=1
                      ))
print(run_correlation(prefix="validation5",
                      queryThreshold=6.49,
                      cpu_MAPE_threshold=1.6,
                      io_threshold=300,
                      bandwidth_threshold=300,
                      bandwidth_MAPE_threshold=1
                      ))
