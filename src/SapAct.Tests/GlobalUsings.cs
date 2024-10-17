﻿global using Azure;
global using Azure.Identity;
global using Azure.Messaging.ServiceBus;
global using Azure.Monitor.Query;
global using Azure.Monitor.Query.Models;
global using Azure.Storage.Blobs;
global using FluentAssertions;
global using Kusto.Data;
global using Kusto.Data.Common;
global using Kusto.Data.Exceptions;
global using Kusto.Data.Net.Client;
global using Microsoft.Data.SqlClient;
global using Microsoft.Extensions.Configuration;
global using Microsoft.Extensions.DependencyInjection;
global using SapAct.Extensions;
global using SapAct.Models;
global using SapAct.Services;
global using SapAct.Tests.Extensions;
global using System.Text;
global using System.Text.Json;
global using Microsoft.Extensions.Logging;
global using Moq;

