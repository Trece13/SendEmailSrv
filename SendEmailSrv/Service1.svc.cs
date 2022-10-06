using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;
using System.Web.Configuration;

namespace SendEmailSrv
{
    // NOTA: puede usar el comando "Rename" del menú "Refactorizar" para cambiar el nombre de clase "Service1" en el código, en svc y en el archivo de configuración.
    // NOTE: para iniciar el Cliente de prueba WCF para probar este servicio, seleccione Service1.svc o Service1.svc.cs en el Explorador de soluciones e inicie la depuración.
    public class Service1 : IService1
    {
        public void SendMail(string subject, string body)
        {
            string EmailSource = WebConfigurationManager.AppSettings["Email"];
            string EmailDestination = WebConfigurationManager.AppSettings["EmailDestination"];
            string Password = WebConfigurationManager.AppSettings["Password"];

            MailMessage oMailMessage = new MailMessage(EmailSource, EmailDestination, subject, body);
            oMailMessage.IsBodyHtml = true;
            SmtpClient oSmtClient = new SmtpClient(WebConfigurationManager.AppSettings["Host"]);
            oSmtClient.EnableSsl = true;
            oSmtClient.UseDefaultCredentials = false;
            oSmtClient.Host = WebConfigurationManager.AppSettings["Host"];
            oSmtClient.Port = Convert.ToInt32(WebConfigurationManager.AppSettings["Port"]);
            oSmtClient.Credentials = new System.Net.NetworkCredential(EmailSource, Password);

            oSmtClient.Send(oMailMessage);
            oSmtClient.Dispose();
        }

        public CompositeType GetDataUsingDataContract(CompositeType composite)
        {
            if (composite == null)
            {
                throw new ArgumentNullException("composite");
            }
            if (composite.BoolValue)
            {
                composite.StringValue += "Suffix";
            }
            return composite;
        }
    }
}
