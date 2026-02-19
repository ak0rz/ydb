#include "actor_class_stats.h"

namespace NActors {

    TActorClassStatsTable& GetActorClassStatsTable() {
        static TActorClassStatsTable instance;
        return instance;
    }

} // namespace NActors
