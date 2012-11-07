/*
 Copyright 2000-2012  Laboratory of Neuro Imaging (LONI), <http://www.LONI.ucla.edu/>.

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

import java.util.logging.Logger;
import org.ggf.drmaa.InvalidJobException;
import org.ggf.drmaa.JobInfo;
import org.ggf.drmaa.Session;
import plgrid.event.EventFinished;

/**
 *
 * @author Petros Petrosyan
 */
public class DRMAAJobFinishListener extends Thread {

    private DRMAAPlugin plugin;
    private static final Logger logger = Logger.getLogger(DRMAAJobFinishListener.class.getName());

    public DRMAAJobFinishListener(DRMAAPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void run() {

        while (true) {

            JobInfo info = null;

            try {
                info = plugin.getSession().wait(Session.JOB_IDS_SESSION_ANY, Session.TIMEOUT_WAIT_FOREVER);
            } catch (InvalidJobException ex) {
                try {
                    Thread.sleep(1000);
                } catch (Exception ex2) {
                    ex2.printStackTrace();
                }

                continue;
                /**
                 * For some reason when a job gets deleted by the drmaa engine,
                 * session.wait(String, long) throws a null pointer exception
                 */
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            if (info != null) {
                try {
                    System.out.println("!!!! Job finished " + info.getJobId());
                    EventFinished ef = new EventFinished(info.getJobId(), "1",
                            System.currentTimeMillis(), info.getExitStatus());
                    plugin.fireEvent(ef);

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
