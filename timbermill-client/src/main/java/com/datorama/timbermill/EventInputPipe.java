package com.datorama.timbermill;

import java.util.List;

public interface EventInputPipe {

    List<Event> read(int maxEvent);

}
