using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Windows.Forms.VisualStyles;
using Beanstalkd.Client.Default;

namespace Beanstalkd.Client.Tests
{
    public partial class TestForm : Form
    {
        private const string TestTube = "test";
        private readonly IBeanstalkdClient _beanstalkdClient;
        private bool _isStoping;

        public TestForm()
        {
            InitializeComponent();

            _beanstalkdClient = ManagedBeanstalkdClientFactory.Create("192.168.1.254");
            _beanstalkdClient.Watch(TestTube);
            _beanstalkdClient.Ignore("default");

            Task.Factory.StartNew(PullJobProcedure);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            _beanstalkdClient.Put(TestTube, Encoding.UTF8.GetBytes(DateTime.Now.ToLongTimeString()));
        }

        private void PullJobProcedure()
        {
            while (!_isStoping)
            {
                try
                {
                    Job job;
                    //wait job for two seconds
                    if (_beanstalkdClient.Reserve(2, out job) != ReserveStatus.Reserved) continue;

                    listBox1.Invoke((Action)(() => listBox1.Items.Add(Encoding.UTF8.GetString(job.Data))));
                    _beanstalkdClient.Delete(job.JobId);
                }
                catch (Exception ex)
                {

                    listBox1.Invoke((Action) (() => listBox1.Items.Add(ex.ToString())));
                }
            }
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            _isStoping = true;

            base.OnClosing(e);
        }
    }
}
