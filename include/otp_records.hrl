%% 'DOWN' and 'EXIT' records to simplify matching.

-record('DOWN', {
    ref,   % monitor reference
    type,  % 'process'
    pid,   % pid
    reason % reason for termination
}).

-record('EXIT', {
    pid,   % pid
    reason % reason for termination
}).

-record(timeout, {
    ref, % timer reference
    msg  % timer msg
}).
