/*
 Copyright 2000-2013  Laboratory of Neuro Imaging (LONI), <http://www.LONI.ucla.edu/>.

 This file is part of the LONI Pipeline Plug-ins (LPP), not the LONI Pipeline itself;
 see <http://pipeline.loni.ucla.edu/>.

 This plug-in program (not the LONI Pipeline) is free software: you can redistribute it
 and/or modify it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or  (at your option)
 any later version. The LONI Pipeline <http://pipeline.loni.ucla.edu/> has a different
 usage license <http://www.loni.ucla.edu/Policies/LONI_SoftwareAgreement.shtml>.

 This plug-in program is distributed in the hope that it will be useful, but WITHOUT ANY
 WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. 

 If you make improvements, modifications and extensions of the LONI Pipeline Plug-ins
 software,  you agree to share them with the LONI Pipeline developers and the broader   
 community according to the GPL license.
 */
package drmaaplugin;

import java.util.List;
import org.ggf.drmaa.DrmCommunicationException;
import org.ggf.drmaa.InvalidJobException;
import org.ggf.drmaa.Session;
import plgrid.event.EventRunning;

/**
 *
 * @author Petros Petrosyan
 */
public class DRMAAJobStatusChecker extends Thread {

    private DRMAAPlugin plugin;

    public DRMAAJobStatusChecker(DRMAAPlugin plugin) {
        this.plugin = plugin;
        setName("DRMAAJobStatusChecker");
    }

    @Override
    public void run() {

        Session session = plugin.getSession();

        while (true) {
            try {
                List<String> queuedJobs = plugin.getQueuedJobs();

                for (String jobID : queuedJobs) {

                    int status = -1;
                    try {
                        System.out.println("Getting status of job " + jobID);
                        status = session.getJobProgramStatus(jobID);

                        System.out.println("Status: " + status);
                        if (status != Session.DONE) {

                            if (status == Session.RUNNING) {
                                plugin.fireEvent(new EventRunning(jobID, "1"));
                                plugin.removeQueuedJob(jobID);
                            } else {
                                //logger.log(Level.SEVERE, "getJobProgramStatus of " + job + " returned status: " + status);
                            }
                        }
                    } catch (DrmCommunicationException ex) {

                        if (ex.getMessage().contains("GDI mismatch")) {
                            // ignore this
                        } else {
                            System.err.println("Unable to getJobProgramStatus of job " + jobID);
                            ex.printStackTrace();
                        }
                    } catch (InvalidJobException ex) {
                        plugin.removeQueuedJob(jobID);
                    } catch (Exception ex) {
                        System.err.println("Unable to getJobProgramStatus of job " + jobID);
                        ex.printStackTrace();
                    }
                }
                sleep(1000);

            } catch (InterruptedException ex) {
                System.err.println("DRMAA Plugin Job Status Checker failure");
                ex.printStackTrace();
            }
        }
    }
}