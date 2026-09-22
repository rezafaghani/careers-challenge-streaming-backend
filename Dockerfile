FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .editorconfig ./
COPY src/StreamingBackend/StreamingBackend.csproj src/StreamingBackend/
RUN dotnet restore src/StreamingBackend/StreamingBackend.csproj
COPY src/StreamingBackend/*.cs src/StreamingBackend/
RUN dotnet publish src/StreamingBackend/StreamingBackend.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "StreamingBackend.dll"]
