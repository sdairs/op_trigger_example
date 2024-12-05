import { logger, schedules, wait } from "@trigger.dev/sdk/v3";

// You'll need to store this securely in your environment variables
const TINYBIRD_TOKEN = process.env.TINYBIRD_TOKEN;

export const tinybirdCopyJob = schedules.task({
  id: "copy_job",
  run: async (payload, { ctx }) => {
    if (!TINYBIRD_TOKEN) {
      throw new Error("Tinybird API token not found");
    }
    if (!payload.pipeId) {
      throw new Error("Pipe ID not found");
    }

    // Start the copy job
    try {
      const copyJobResponse = await fetch(
        `https://api.tinybird.co/v0/pipes/${payload.pipeId}/copy`,
        {
          method: "POST",
          headers: {
            'Authorization': `Bearer ${TINYBIRD_TOKEN}`
          }
        }
      ).then((r) => r.json());

      if (!copyJobResponse || !('job' in copyJobResponse)) {
        console.log(copyJobResponse);
        throw new Error('Invalid response from Tinybird API');
      }

      const jobId = copyJobResponse.job.job_id;
      logger.log("Copy job started", { jobId });

      // Check job status in a loop
      let jobStatus = '';
      const maxAttempts = 30;
      let attempts = 0;

      while (attempts < maxAttempts) {
        const statusResponse = await fetch(
          `https://api.tinybird.co/v0/jobs/${jobId}`,
          {
            headers: {
              'Authorization': `Bearer ${TINYBIRD_TOKEN}`
            }
          }
        ).then((r) => r.json());

        jobStatus = statusResponse.status;

        logger.log("Job status check", { jobId, status: jobStatus, attempt: attempts + 1 });

        if (jobStatus === 'done') {
          logger.log("Copy job completed successfully", { jobId });
          return { success: true, jobId };
        }

        if (jobStatus === 'error' || jobStatus === 'failed') {
          throw new Error(`Copy job failed with status: ${jobStatus}`);
        }

        // Wait for 5 seconds before checking again
        await wait.for({ seconds: 5 });
        attempts++;
      }

      throw new Error(`Job timed out after ${maxAttempts} attempts`);
    } catch (error) {
      console.log(error)
      logger.error("Error in Tinybird copy job", { error });
      throw error;
    }
  },
});