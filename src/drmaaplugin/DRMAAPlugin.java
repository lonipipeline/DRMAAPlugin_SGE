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

import drmaaplugin.accounting.ARCODatabase;
import drmaaplugin.accounting.SGEAccountingThread;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import org.ggf.drmaa.*;
import plgrid.GridJobArgument;
import plgrid.GridJobInfo;
import plgrid.GridJobSubmitInfo;
import plgrid.PipelineGridPlugin;

/**
 * This is a LONI Pipeline's Grid Plugin class. It provides communication with
 * grid manager.
 *
 * This plugin is designed to work with Sun Grid Engine.
 *
 * @author Petros Petrosyan
 */
public class DRMAAPlugin extends PipelineGridPlugin {

    public DRMAAPlugin() {

        SGE_ROOT = System.getenv("SGE_ROOT");
        SGE_CELL = System.getenv("SGE_CELL");
        String sge_port = System.getenv("SGE_PORT");

        if (sge_port == null) {
            SGE_PORT = "6444"; // use default port if env. variable is not set
        } else {
            SGE_PORT = sge_port;
        }

        System.out.println("SGE Root: " + SGE_ROOT);
        System.out.println("SGE Cell: " + SGE_CELL);
        System.out.println("SGE Port: " + SGE_PORT);

        //set session
        try {
            session = SessionFactory.getFactory().getSession();

            session.init("session=loniPipelineServer_DRMAAPlugin");

            DRMAAJobFinishListener l = new DRMAAJobFinishListener(this);
            l.start();

        } catch (java.lang.UnsatisfiedLinkError ule) {
            System.err.println("DRMAA Library can't be found.");
            ule.printStackTrace();
            return;
        } catch (Exception e) {
            System.err.println("Unable to initialize DRMAA session.");
            e.printStackTrace();
            return;
        }

        System.out.println("DRMAAPlugin (version: " + DRMAA_PLUGIN_VERSION + ") started.");
        queuedJobs = Collections.synchronizedList(new LinkedList<String>());

        DRMAAJobStatusChecker jsc = new DRMAAJobStatusChecker(this);
        jsc.start();
    }

    public Session getSession() {
        return session;
    }

    public List<String> getQueuedJobs() {
        List<String> ret = new LinkedList<String>();

        ret.addAll(queuedJobs);

        return ret;
    }

    public void removeQueuedJob(String jobId) {
        if (jobId != null) {
            queuedJobs.remove(jobId);
        }
    }

    @Override
    public String submitJob(GridJobSubmitInfo gji) {
        try {
            // Create and configure DRMAA JobTemplate
            JobTemplate jt = session.createJobTemplate();

            String outputPath = gji.getOutputPath();
            String errorPath = gji.getErrorPath();

            if (gji.getSubmissionType() == GridJobSubmitInfo.SUBMISSION_ARRAY) {
                int lastIndexUnd = outputPath.lastIndexOf("_");
                int lastIndexDot = outputPath.lastIndexOf(".");
                outputPath = outputPath.substring(0, lastIndexUnd + 1)
                        + JobTemplate.PARAMETRIC_INDEX
                        + outputPath.substring(lastIndexDot);

                lastIndexUnd = errorPath.lastIndexOf("_");
                lastIndexDot = errorPath.lastIndexOf(".");
                errorPath = errorPath.substring(0, lastIndexUnd + 1)
                        + JobTemplate.PARAMETRIC_INDEX
                        + errorPath.substring(lastIndexDot);
            }

            jt.setOutputPath("localhost:" + outputPath);
            jt.setErrorPath("localhost:" + errorPath);

            if (gji.getEnvironmentProperties() != null) {
                jt.setJobEnvironment(gji.getEnvironmentProperties());
            }

            jt.setNativeSpecification(gji.getNativeSpecification());


            String executableLocation = gji.getCommand();

            if (executableLocation == null) {
                throw new Exception("Failed to get Executable Location");
            }

            String username = gji.getUsername();
            if (username == null) {
                throw new Exception("Failed to get Username");
            }

            List<GridJobArgument> arguments = gji.getArguments();

            if (arguments == null || arguments.contains(null)) {
                throw new Exception("Failed to get command line arguments");
            }

            LinkedList<String> args = new LinkedList<String>();

            // Add sudo -u username if privilegeEscalation is set to true
            if (gji.getPrivilegeEscalation()) {
                jt.setRemoteCommand("sudo");
                args.add("-u");
                args.add(gji.getUsername());
                args.add(gji.getCommand());
            } else {
                jt.setRemoteCommand(gji.getCommand());
            }

            for (GridJobArgument arg : arguments) {
                String argValue = arg.getValue();
                if (argValue != null) {
                    args.add(argValue);
                }
            }
            
            jt.setArgs(args);

            if (gji.getSubmissionType() == GridJobSubmitInfo.SUBMISSION_ARRAY) {
                List<String> jobIDs = session.runBulkJobs(jt, gji.getBeginIndex(), gji.getEndIndex(), 1);

                String ret = "ERROR: Failed to submit jobs";

                if (!jobIDs.isEmpty()) {
                    ret = jobIDs.get(0) + "-" + gji.getEndIndex() + ":1";
                    for (String jobID : jobIDs) {
                        queuedJobs.add(jobID);
                    }
                }

                session.deleteJobTemplate(jt);

                return ret;
            } else {
                // Submit the job
                String jobID = session.runJob(jt);

                queuedJobs.add(jobID);

                session.deleteJobTemplate(jt);

                return jobID;
            }

        } catch (Exception ex) {
            StringBuilder error = new StringBuilder("ERROR: Unable to submit job. Internal error occurred\n");

            error.append("\n     Date: " + new Date().toString());
            error.append("\n   Reason: " + ex.getMessage());
            System.err.println(error.toString());
            ex.printStackTrace();
            return error.toString();
        }
    }

    @Override
    public void killJob(String jobId, String username, boolean force) {

        try {
            session.control(jobId, Session.TERMINATE);
        } catch (InvalidJobException ex) {
            /**
             * This doesn't matter
             */
        } catch (Exception ex) {
            System.err.println("Unable to delete job ");
            ex.printStackTrace();
        }
    }

    @Override
    public List<GridJobInfo> getJobList(String complexVariables) {
        List<GridJobInfo> ret = new LinkedList<GridJobInfo>();

        StringBuilder cmd = new StringBuilder("qstat");

        if (complexVariables != null && complexVariables.length() > 0) {
            cmd.append(" -l ");
            cmd.append(complexVariables);
        }

        StringTokenizer st = new StringTokenizer(cmd.toString());
        String[] command = new String[st.countTokens()];

        for (int k = 0; k < command.length; k++) {
            command[k] = st.nextToken();
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = null;
        try {
            process = pb.start();
            InputStreamReader isr = new InputStreamReader(process.getInputStream());
            BufferedReader ibr = new BufferedReader(isr);

            String response;
            int lineNum = 0;
            while ((response = ibr.readLine()) != null) {
                // We don't need first two lines 
                // job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID 
                // -----------------------------------------------------------------------------------------------------------------

                if (lineNum > 1) {
                    String[] tokens = response.split(" ");
                    int index = 0;

                    String jobId = "";
                    String lastToken = "";

                    for (String t : tokens) {
                        if (t.trim().length() == 0) {
                            continue;
                        }

                        if (index == 0) {
                            jobId = t;
                        }

                        lastToken = t;

                        index++;
                    }

                    // lastToken is the taskId
                    ret.add(getJobInfo(jobId + "." + lastToken));
                }
                lineNum++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (process != null) {
                releaseProcess(process);
            }
        }

        return ret;
    }

    @Override
    public GridJobInfo getJobInfo(String jobID) {
        GridJobInfo gji = new GridJobInfo(jobID);
        try {
            int status = session.getJobProgramStatus(jobID);

            System.out.println("Job " + jobID + " has status " + status);

            switch (status) {
                case Session.QUEUED_ACTIVE:
                    gji.setState(GridJobInfo.STATE_QUEUED);
                    break;
                case Session.RUNNING:
                    gji.setState(GridJobInfo.STATE_RUNNING);

                    break;
                default:
                    System.err.println("Unexpected status: " + status + " for job " + jobID);
                    break;
            }

        } catch (InvalidJobException ex) {
            gji = getFinishedJobInfo(jobID);
        } catch (DrmaaException ex) {
            System.err.println("ERROR: " + ex.getMessage());
            ex.printStackTrace();
        }

        return gji;
    }

    private GridJobInfo getFinishedJobInfo(String jobId) {
        GridJobInfo fji = null;
        if (finishedJobRetrievalMethod == null || finishedJobRetrievalMethod.trim().length() == 0) {
            fji = sgeAccountingThread.getFinishedJobInfo(jobId);
        } else if (finishedJobRetrievalMethod.toLowerCase().equals("arco")) {
            if (arcoDatabase == null) {
                arcoDatabase = new ARCODatabase(this);
            }
            fji = arcoDatabase.getFinishedJobInfo(jobId);
        } else {
            System.err.println("Method \"" + finishedJobRetrievalMethod + "\" is not supported by this plugin for obtaining finished job information.");
        }

        return fji;
    }

    @Override
    public void setPreferences(Map<String, String> prefs) {
        super.setPreferences(prefs);

        finishedJobRetrievalMethod = prefs.get("GridFinishedJobRetrievalMethod");

        if (finishedJobRetrievalMethod == null || finishedJobRetrievalMethod.trim().length() == 0) {

            if (arcoDatabase != null) {
                // turn off ARCo if it is on.
                arcoDatabase = new ARCODatabase(this);
                arcoDatabase.shutdown();
            }

            if (sgeAccountingThread == null) {
                // turn on SGE Accounting thread if it is off.
                sgeAccountingThread = new SGEAccountingThread(SGE_ROOT, SGE_CELL);
                sgeAccountingThread.start();
            }
        } else if (finishedJobRetrievalMethod.toLowerCase().equals("arco")) {
            if (sgeAccountingThread != null) {
                // turn off SGE Accounting thread if it is on.
                sgeAccountingThread.shutdown();
            }

            if (arcoDatabase == null) {
                // turn on SGE ARCo if it is off.
                arcoDatabase = new ARCODatabase(this);
            }
        }
    }

    private void releaseProcess(Process p) {
        Exception ex = null;

        if (p != null) {
            try {
                p.getInputStream().close();
            } catch (Exception iex) {
                ex = iex;
            }
            try {
                p.getOutputStream().close();
            } catch (Exception oex) {
                ex = oex;
            }
            try {
                p.getErrorStream().close();
            } catch (Exception eex) {
                ex = eex;
            }
            try {
                p.destroy();
            } catch (Exception dex) {
                ex = dex;
            }
        }

        if (ex != null) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("PipelineGridPlugin version " + PipelineGridPlugin.VERSION);
        System.out.println("DRMAA Plugin version " + DRMAA_PLUGIN_VERSION);
    }
    private final String SGE_ROOT;
    private final String SGE_CELL;
    private final String SGE_PORT;
    private Session session;
    private List<String> queuedJobs;
    private String finishedJobRetrievalMethod;
    private ARCODatabase arcoDatabase;
    private SGEAccountingThread sgeAccountingThread;
    public static final String DRMAA_PLUGIN_VERSION = "3.0";
}
