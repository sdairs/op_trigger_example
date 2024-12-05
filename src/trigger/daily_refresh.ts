import { logger, schedules, wait } from "@trigger.dev/sdk/v3";
import { tinybirdCopyJob, runs } from "./copy_job";


const TINYBIRD_TOKEN = process.env.TINYBIRD_TOKEN;

// Copy pipe IDs
const TINYBIRD_TOP_USERS_PIPE_ID = process.env.TINYBIRD_TOP_USERS_PIPE_ID;
const TINYBIRD_AGGDS_PIPE_ID = process.env.TINYBIRD_AGGDS_PIPE_ID;

export const dailyRefresh = schedules.task({
    id: "daily_refresh",
    cron: "0 9 * * *", // Adjust the schedule as needed
    run: async (payload, { ctx }) => {
        if (!TINYBIRD_TOKEN) {
            throw new Error("Tinybird API token not found");
        }

        try {

            // Building the aggregated_data_sources ds
            const agg_ds_result = await tinybirdCopyJob.triggerAndWait({
                pipeId: TINYBIRD_AGGDS_PIPE_ID
            });

            // Top users
            const top_users_result = await tinybirdCopyJob.triggerAndWait({
                pipeId: TINYBIRD_TOP_USERS_PIPE_ID
            });

        } catch (error) {
            logger.error("Error in Tinybird copy job", { error });
            throw error;
        }
    },
});