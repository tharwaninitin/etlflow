(this["webpackJsonpetlflow-updated"]=this["webpackJsonpetlflow-updated"]||[]).push([[0],{30:function(t,e,n){},47:function(t,e,n){"use strict";n.r(e);var s=n(1),a=n.n(s),i=n(32),o=n.n(i),r=n(6),c=n(7),l=n(14),d=n(9),b=n(8),u=n(3),j=n.n(u),h=n(10),p=(n(16),n(17),n(24),n(30),n(20),n(21),n(5)),m=n.n(p),f=n(0),O=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).state={jobs:[],notification:!1,run_job_props:"",job_name:""},s}return Object(c.a)(n,[{key:"updateJobActiveState",value:function(){var t=Object(h.a)(j.a.mark((function t(e,n){var s,a,i,o;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/etlflow",s=localStorage.getItem("Authorization"),t.next=4,fetch("/api/etlflow",{method:"POST",headers:{content_type:"applcation/json",Authorization:s},body:JSON.stringify({query:'\n            mutation {\n              update_job_state (name:"'.concat(e,'" ,state: ').concat(n,"){\n             }\n           }\n          ")})});case 4:return 403===(a=t.sent).status?console.log("user not found"):console.log("user  found"),t.next=8,a.json();case 8:return i=t.sent,o=i.data,t.abrupt("return",o);case 11:case"end":return t.stop()}}),t)})));return function(e,n){return t.apply(this,arguments)}}()},{key:"run_etl_job",value:function(){var t=Object(h.a)(j.a.mark((function t(e,n){var s,a,i,o;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/etlflow",s=localStorage.getItem("Authorization"),t.next=4,fetch("/api/etlflow",{method:"POST",headers:{content_type:"applcation/json",Authorization:s},body:JSON.stringify({query:'\n            mutation {\n              run_job (name:"'.concat(e,'" ,props: { key: "", value: "" }){\n                name\n                props {key value}\n             }\n           }\n          ')})});case 4:return 403===(a=t.sent).status&&(console.log("user not found"),localStorage.removeItem("Authorization")),t.next=8,a.json();case 8:return i=t.sent,o=i.data,t.abrupt("return",o);case 11:case"end":return t.stop()}}),t)})));return function(e,n){return t.apply(this,arguments)}}()},{key:"componentDidMount",value:function(){m()("#joblist").DataTable()}},{key:"show_modal",value:function(t){var e="";t.map((function(t){return e+="<b>"+t.key+"</b>="+t.value+"<br></br>"})),m()("#jobListeModal .modal-body").html(e),m()("#jobListeModal").modal()}},{key:"set_notification",value:function(t){this.setState((function(e){return{notification:t}}))}},{key:"render",value:function(){var t=this,e="",n="";return this.state.notification&&(n=Object(f.jsxs)("div",{class:"alert alert-success alert-dismissible fade show",role:"alert",children:[Object(f.jsxs)("h4",{class:"alert-heading",children:["Job ",this.state.job_name.replace(/^"(.*)"$/,"$1")," submitted successfully!  "]}),Object(f.jsx)("p",{children:this.state.run_job_props}),Object(f.jsx)("button",{type:"button",class:"close","data-dismiss":"alert","aria-label":"Close",onClick:function(){t.set_notification(!1)},children:Object(f.jsx)("span",{"aria-hidden":"true",children:"\xd7"})})]})),Object(f.jsxs)("div",{class:"container-fluid",children:[Object(f.jsx)("div",{children:n}),Object(f.jsxs)("table",{id:"joblist",class:"table table-sm table-bordered",children:[Object(f.jsx)("thead",{class:"thead-dark",children:Object(f.jsxs)("tr",{children:[Object(f.jsx)("th",{children:"Active/Inactive"}),Object(f.jsx)("th",{children:"Job Name"}),Object(f.jsx)("th",{children:"Schedule"}),Object(f.jsx)("th",{children:"Next Schedule"}),Object(f.jsx)("th",{children:"Schdule Remaining Time"}),Object(f.jsx)("th",{children:"Job Deploy Mode"}),Object(f.jsx)("th",{children:"Properties"}),Object(f.jsx)("th",{children:"Action"})]})}),Object(f.jsx)("tbody",{children:this.props.jobs.map((function(n){return Object(f.jsxs)("tr",{children:[Object(f.jsx)("td",{children:n.is_active?Object(f.jsx)("div",{className:"checkbox",children:Object(f.jsx)("input",{type:"checkbox","aria-describedby":"UsernameHelp","data-toggle":"toggle",defaultChecked:!0,onClick:function(e){return t.updateJobActiveState(n.name,!1)}})}):Object(f.jsx)("div",{className:"checkbox",children:Object(f.jsx)("input",{type:"checkbox","aria-describedby":"UsernameHelp","data-toggle":"toggle",onClick:function(e){return t.updateJobActiveState(n.name,!0)}})})}),Object(f.jsx)("td",{children:"NA"===n.job_deploy_mode?Object(f.jsx)("div",{children:Object(f.jsx)("p",{className:"text-danger",children:n.name})}):Object(f.jsx)("div",{children:n.name})}),Object(f.jsx)("td",{children:n.schedule}),Object(f.jsx)("td",{children:n.nextSchedule}),Object(f.jsx)("td",{children:n.schduleRemainingTime}),Object(f.jsx)("td",{children:n.job_deploy_mode}),Object(f.jsxs)("td",{children:[Object(f.jsx)("button",{type:"button",class:"btn btn-secondary btn-sm","data-toggle":"modal",onClick:function(){return t.show_modal(n.props)},children:"Properties"}),Object(f.jsx)("div",{class:"modal fade",id:"jobListeModal",role:"dialog","aria-labelledby":"exampleModalLabel","aria-hidden":"true",children:Object(f.jsx)("div",{class:"modal-dialog",role:"document",children:Object(f.jsxs)("div",{class:"modal-content",children:[Object(f.jsxs)("div",{class:"modal-header",children:[Object(f.jsx)("h5",{class:"modal-title",id:"exampleModalLabel",children:"Properties "}),Object(f.jsx)("button",{type:"button",class:"close","data-dismiss":"modal","aria-label":"Close",children:Object(f.jsx)("span",{"aria-hidden":"true",children:"\xd7"})})]}),Object(f.jsx)("div",{class:"modal-body"}),Object(f.jsx)("div",{class:"modal-footer",children:Object(f.jsx)("button",{type:"button",class:"btn btn-secondary","data-dismiss":"modal",children:"Close"})})]})})})]}),Object(f.jsx)("td",{children:Object(f.jsx)("input",{type:"button",value:"Run",class:"btn btn-secondary btn-sm",onClick:function(){t.run_etl_job(n.name).then((function(n){var s=n.run_job;console.log(JSON.parse(JSON.stringify(s))),t.set_notification(!0);var a=JSON.parse(JSON.stringify(s));for(var i in a)"props"===i?(e=JSON.stringify(a[i]),t.setState((function(t){return{run_job_props:e}}))):t.setState((function(t){return{job_name:JSON.stringify(a[i])}}))}))}})})]})}))})]})]})}}]),n}(a.a.Component),x=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).state={stepruns:[]},s}return Object(c.a)(n,[{key:"fetchEtlFlowJobById",value:function(){var t=Object(h.a)(j.a.mark((function t(e){var n,s,a,i;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/etlflow",n=localStorage.getItem("Authorization"),console.log("job_run_id :"+e),t.next=5,fetch("/api/etlflow",{method:"POST",headers:{content_type:"applcation/json",Authorization:n},body:JSON.stringify({query:'\n          query {\n            stepruns(job_run_id:    "'.concat(e,'"){\n                job_run_id\n                step_name\n                properties\n                state\n                start_time\n                elapsed_time\n                step_type\n                step_run_id\n             }\n           }\n          ')})});case 5:return 403===(s=t.sent).status&&(console.log("user not found"),localStorage.removeItem("Authorization")),t.next=9,s.json();case 9:return a=t.sent,i=a.data,t.abrupt("return",i);case 12:case"end":return t.stop()}}),t)})));return function(e){return t.apply(this,arguments)}}()},{key:"setInterval",value:function(t){function e(){return t.apply(this,arguments)}return e.toString=function(){return t.toString()},e}((function(){var t=this;this.timerID=setInterval((function(){return t.fetchEtlFlowJobs()}),1e6)}))},{key:"componentDidMount",value:function(){var t=this;this.fetchEtlFlowJobById(this.props.job_run_id).then((function(e){var n=e.stepruns;console.log(n),t.setState((function(t){return{stepruns:n}}))})),this.setInterval()}},{key:"componentWillUnmount",value:function(){clearInterval(this.timerID)}},{key:"show_modal",value:function(t){var e="",n=JSON.parse(t);for(var s in console.log("data_jobruns :"+n),n)e+="<b>"+s+"</b>="+n[s]+"<br></br>",console.log("result is :"+s+" value: "+n[s]),m()("#stepRunModal .modal-body").html(e),m()("#stepRunModal").modal()}},{key:"show_step_run_stat_modal",value:function(t){console.log("data state  :"+t),m()("#stepRunStateModal .modal-body").html(t),m()("#stepRunStateModal").modal()}},{key:"render",value:function(){var t=this,e=!1;this.state.stepruns.map((function(t){""!==t.step_run_id&&(e=!0)}));var n="";n=e?Object(f.jsx)("th",{children:"Linked Job"}):"";var s=["EtlFlowJobStep","DPSparkJobStep"];return Object(f.jsx)("div",{className:"container-fluid",children:Object(f.jsxs)("table",{id:"job runs",class:"table table-sm table-bordered",children:[Object(f.jsx)("thead",{class:"thead-dark",children:Object(f.jsxs)("tr",{children:[Object(f.jsx)("th",{children:"Job Run Id"}),Object(f.jsx)("th",{children:"Step Type"}),Object(f.jsx)("th",{children:"Step Name"}),Object(f.jsx)("th",{children:"Start Time"}),Object(f.jsx)("th",{children:"Elapsed Time"}),Object(f.jsx)("th",{children:"Status"}),Object(f.jsx)("th",{children:"Properties"}),n,Object(f.jsx)("th",{children:Object(f.jsx)("button",{className:"btn btn-secondary btn-block",onClick:function(){return t.props.set_job_run_state()},children:"Back"})})]})}),Object(f.jsx)("tbody",{children:this.state.stepruns.map((function(e){return Object(f.jsxs)("tr",{children:[Object(f.jsx)("td",{children:e.job_run_id}),Object(f.jsx)("td",{children:e.step_type}),Object(f.jsx)("td",{children:e.step_name}),Object(f.jsx)("td",{children:e.start_time}),Object(f.jsx)("td",{children:e.elapsed_time}),Object(f.jsx)("td",{children:"pass"===e.state||""==e.state?Object(f.jsx)("div",{children:Object(f.jsx)("p",{className:"text-success",children:e.state})}):"started"===e.state||"running"==e.state?Object(f.jsx)("div",{children:Object(f.jsx)("p",{className:"text-warning",children:e.state})}):void 0}),Object(f.jsxs)("td",{children:[Object(f.jsx)("button",{type:"button",class:"btn btn-secondary btn-sm","data-toggle":"modal",onClick:function(){return t.show_modal(e.properties)},children:"Properties"}),Object(f.jsx)("div",{class:"modal fade",id:"stepRunModal",tabindex:"-1",role:"dialog","aria-labelledby":"exampleModalLabel","aria-hidden":"true",children:Object(f.jsx)("div",{class:"modal-dialog",role:"document",children:Object(f.jsxs)("div",{class:"modal-content",children:[Object(f.jsxs)("div",{class:"modal-header",children:[Object(f.jsx)("h5",{class:"modal-title",id:"exampleModalLabel",children:"Step Properties"}),Object(f.jsx)("button",{type:"button",class:"close","data-dismiss":"modal","aria-label":"Close",children:Object(f.jsx)("span",{"aria-hidden":"true",children:"\xd7"})})]}),Object(f.jsx)("div",{class:"modal-body"}),Object(f.jsx)("div",{class:"modal-footer",children:Object(f.jsx)("button",{type:"button",class:"btn btn-secondary","data-dismiss":"modal",children:"Close"})})]})})})]}),Object(f.jsx)("td",{children:s.includes(e.step_type)?Object(f.jsx)("input",{type:"button",value:"Check Progress",class:"btn btn-secondary btn-sm",onClick:function(){return t.fetchEtlFlowJobById(e.step_run_id).then((function(e){var n=e.stepruns;console.log(n),t.setState((function(t){return{stepruns:n}}))}))}}):Object(f.jsx)("div",{})})]})}))})]})})}}]),n}(a.a.Component),_=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).resetForm=function(){s.setState({jobruns:s.state.jobruns,filter:"",job_name:"",filter_start_date:"",filter_end_date:"",paginationValue:0,stepRun:!1})},s.job_run_id="",s.filterOperation=["IN","NOT IN"],s.state={jobruns:[],filter:"",job_name:"",filter_start_date:"",filter_end_date:"",paginationValue:0,stepRun:!1},s.baseState=s.state,s.set_job_run_state=s.set_job_run_state.bind(Object(l.a)(s)),s}return Object(c.a)(n,[{key:"fetchEtlFlowJobs",value:function(){var t=Object(h.a)(j.a.mark((function t(){var e,n,s,a,i,o,r;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/etlflow",e=localStorage.getItem("Authorization"),n=10,s=this.state.paginationValue,a=" query { jobruns(limit: ".concat(n," , offset: ").concat(s,"){ job_run_id job_name description properties state start_time elapsed_time job_type is_master}}"),a=""!==this.state.filter_end_date&&""!==this.state.filter?" query { jobruns(limit: ".concat(n," , offset: ").concat(s,',  startTime: "').concat(this.state.filter_start_date,'", endTime: "').concat(this.state.filter_end_date,'",filter: "').concat(this.state.filter,'", jobName: "').concat(this.state.job_name,'"  )\n      { job_run_id job_name description properties state start_time elapsed_time job_type is_master}}'):""!==this.state.filter_end_date?" query { jobruns(limit: ".concat(n," , offset: ").concat(s,',  startTime: "').concat(this.state.filter_start_date,'", endTime: "').concat(this.state.filter_end_date,'")\n      { job_run_id job_name description properties state start_time elapsed_time job_type is_master}}'):""!==this.state.filter?" query { jobruns(limit: ".concat(n," , offset: ").concat(s,', filter: "').concat(this.state.filter,'", jobName: "').concat(this.state.job_name,'"  )\n      { job_run_id job_name description properties state start_time elapsed_time job_type is_master}}'):""===this.state.filter&&""!==this.state.job_name?" query { jobruns(limit: ".concat(n," , offset: ").concat(s,' ,filter: "IN", jobName: "').concat(this.state.job_name,'"  )\n      { job_run_id job_name description properties state start_time elapsed_time job_type is_master}}'):" query { jobruns(limit: ".concat(n," , offset: ").concat(s,"){ job_run_id job_name description properties state start_time elapsed_time job_type is_master}}"),console.log("Query is :"+a),t.next=9,fetch("/api/etlflow",{method:"POST",headers:{content_type:"applcation/json",Authorization:e},body:JSON.stringify({query:a})});case 9:return 403===(i=t.sent).status&&(console.log("user not found"),localStorage.removeItem("Authorization")),t.next=13,i.json();case 13:return o=t.sent,r=o.data,t.abrupt("return",r);case 16:case"end":return t.stop()}}),t,this)})));return function(){return t.apply(this,arguments)}}()},{key:"setInterval",value:function(t){function e(){return t.apply(this,arguments)}return e.toString=function(){return t.toString()},e}((function(){var t=this;this.timerID=setInterval((function(){return t.fetchEtlFlowJobs()}),1e6)}))},{key:"componentDidMount",value:function(){var t=this;this.fetchEtlFlowJobs().then((function(e){var n=e.jobruns;console.log(n),t.setState((function(t){return{jobruns:n}}))})),this.setInterval()}},{key:"componentWillUnmount",value:function(){clearInterval(this.timerID)}},{key:"set_job_run_state",value:function(){this.setState((function(t){return{stepRun:!t.stepRun}}))}},{key:"set_job_name",value:function(){this.setState((function(t){return{job_name:document.getElementById("exampleDataList").value}}))}},{key:"set_filter_start_date",value:function(){this.setState((function(t){return{filter_start_date:document.getElementById("start_date").value}}))}},{key:"set_filter_end_date",value:function(){this.setState((function(t){return{filter_end_date:document.getElementById("end_date").value}}))}},{key:"show_modal",value:function(t){var e="",n=JSON.parse(t);for(var s in console.log("data_jobruns :"+n),n)e+="<b>"+s+"</b>="+n[s]+"<br></br>",console.log("result is :"+s+" value: "+n[s]),m()("#jobRunModal .modal-body").html(e),m()("#jobRunModal").modal()}},{key:"show_job_run_stat_modal",value:function(t){m()("#jobRunStateModal .modal-body").html(t),m()("#jobRunStateModal").modal()}},{key:"render",value:function(){var t=this,e="",n="";return e=0===this.state.paginationValue?Object(f.jsx)("li",{class:"page-item disabled",children:Object(f.jsx)("input",{type:"button",value:"Previous",class:"page-link btn btn-secondary btn-sm"})}):Object(f.jsx)("li",{class:"page-item",children:Object(f.jsx)("input",{type:"button",value:"Previous",class:"page-link btn btn-secondary btn-sm",onClick:function(){t.setState((function(e){return{paginationValue:t.state.paginationValue-10}})),t.fetchEtlFlowJobs(t.state.paginationValue-10).then((function(e){var n=e.jobruns;console.log(n),t.setState((function(t){return{jobruns:n}}))}))}})}),n=""!==this.state.filter?this.state.filter:"Add Filter",this.state.stepRun?Object(f.jsx)("div",{children:Object(f.jsx)(x,{job_run_id:this.job_run_id,set_job_run_state:this.set_job_run_state})}):Object(f.jsxs)("div",{className:"container-fluid",children:[Object(f.jsxs)("ul",{class:"pagination justify-content-center",children:[Object(f.jsxs)("div",{class:"dropdown",children:[Object(f.jsx)("button",{class:"btn btn-light dropdown-toggle",type:"button",id:"dropdownMenuButton","data-toggle":"dropdown","aria-haspopup":"true","aria-expanded":"false",children:n}),Object(f.jsx)("div",{class:"dropdown-menu","aria-labelledby":"dropdownMenuButton",children:this.filterOperation.map((function(e){return Object(f.jsx)("a",{class:"dropdown-item",children:Object(f.jsxs)("button",{class:"btn btn btn-sm",type:"button ",onClick:function(){t.setState((function(t){return{filter:e}}))},children:[" ",e," "]})})}))})]}),Object(f.jsxs)("div",{className:"form-inline float-right",children:[Object(f.jsx)("div",{className:"mb-1",children:Object(f.jsx)("input",{type:"text",className:"form-control input-sm",list:"datalistOptions",id:"exampleDataList",placeholder:"Type to search",onChange:function(e){return t.set_job_name()}})}),Object(f.jsx)("datalist",{id:"datalistOptions",children:this.props.jobs.map((function(t){return Object(f.jsx)("option",{value:t.name})}))})]}),Object(f.jsxs)("div",{class:"form-inline float-right",children:[Object(f.jsx)("div",{class:"mb-1",children:Object(f.jsx)("input",{type:"date",className:"form-control input-sm",placeholder:"startdate(YYYY-MM-DD)",id:"start_date",onChange:function(e){return t.set_filter_start_date()}})}),Object(f.jsx)("div",{class:"mb-1",children:Object(f.jsx)("input",{type:"date",className:"form-control input-sm",placeholder:"enddate(YYYY-MM-DD)",id:"end_date",onChange:function(e){return t.set_filter_end_date()}})})]}),Object(f.jsx)("button",{class:"btn btn btn-sm",type:"button ",onClick:function(){window.location.reload(!0)},children:"Clear"}),Object(f.jsx)("button",{class:"btn btn btn-sm",type:"button ",onClick:function(){t.fetchEtlFlowJobs().then((function(e){var n=e.jobruns;console.log(n),t.setState((function(t){return{jobruns:n}}))}))},children:"Refresh"})]}),Object(f.jsxs)("table",{id:"jobruns",class:"table table-sm table-bordered",children:[Object(f.jsx)("thead",{class:"thead-dark",children:Object(f.jsxs)("tr",{children:[Object(f.jsx)("th",{children:"Job Name"}),Object(f.jsx)("th",{children:"Job Type"}),Object(f.jsx)("th",{children:"Job Start Time"}),Object(f.jsx)("th",{children:"Elapsed Time"}),Object(f.jsx)("th",{children:"Job Status"}),Object(f.jsx)("th",{children:"Job Properties"}),Object(f.jsx)("th",{children:"Step Properties"})]})}),Object(f.jsx)("tbody",{children:this.state.jobruns.map((function(e){return Object(f.jsxs)("tr",{children:[Object(f.jsx)("td",{children:e.job_name}),Object(f.jsx)("td",{children:e.job_type}),Object(f.jsx)("td",{children:e.start_time}),Object(f.jsx)("td",{children:e.elapsed_time}),Object(f.jsx)("td",{children:"pass"===e.state||""==e.state?Object(f.jsx)("div",{children:Object(f.jsx)("p",{className:"text-success",children:e.state})}):"started"===e.state||"running"==e.state?Object(f.jsx)("div",{children:Object(f.jsx)("p",{className:"text-warning",children:e.state})}):Object(f.jsxs)("div",{children:[Object(f.jsx)("button",{type:"button",class:"btn btn-danger btn btn-sm","data-toggle":"modal",onClick:function(){return t.show_job_run_stat_modal(e.state)},children:"Error"}),Object(f.jsx)("div",{class:"modal fade",id:"jobRunStateModal",role:"dialog","aria-labelledby":"exampleModalLabel","aria-hidden":"true",children:Object(f.jsx)("div",{class:"modal-dialog",role:"document",children:Object(f.jsxs)("div",{class:"modal-content",children:[Object(f.jsxs)("div",{class:"modal-header",children:[Object(f.jsx)("h5",{class:"modal-title",id:"exampleModalLabel",children:"Properties "}),Object(f.jsx)("button",{type:"button",class:"close","data-dismiss":"modal","aria-label":"Close",children:Object(f.jsx)("span",{"aria-hidden":"true",children:"\xd7"})})]}),Object(f.jsx)("div",{class:"modal-body"}),Object(f.jsx)("div",{class:"modal-footer",children:Object(f.jsx)("button",{type:"button",class:"btn btn-secondary","data-dismiss":"modal",children:"Close"})})]})})})]})}),Object(f.jsxs)("td",{children:[Object(f.jsx)("button",{type:"button",class:"btn btn-secondary btn-sm","data-toggle":"modal",onClick:function(){return t.show_modal(e.properties)},children:"Properties"}),Object(f.jsx)("div",{class:"modal fade",id:"jobRunModal",role:"dialog","aria-labelledby":"exampleModalLabel","aria-hidden":"true",children:Object(f.jsx)("div",{class:"modal-dialog",role:"document",children:Object(f.jsxs)("div",{class:"modal-content",children:[Object(f.jsxs)("div",{class:"modal-header",children:[Object(f.jsx)("h5",{class:"modal-title",id:"exampleModalLabel",children:"Properties "}),Object(f.jsx)("button",{type:"button",class:"close","data-dismiss":"modal","aria-label":"Close",children:Object(f.jsx)("span",{"aria-hidden":"true",children:"\xd7"})})]}),Object(f.jsx)("div",{class:"modal-body"}),Object(f.jsx)("div",{class:"modal-footer",children:Object(f.jsx)("button",{type:"button",class:"btn btn-secondary","data-dismiss":"modal",children:"Close"})})]})})})]}),Object(f.jsx)("td",{children:Object(f.jsx)("input",{type:"button",value:"Step Properties",class:"btn btn-secondary btn-sm",onClick:function(){t.set_job_run_state(),t.job_run_id=e.job_run_id}})})]})}))})]}),Object(f.jsx)("nav",{"aria-label":"Page navigation example",children:Object(f.jsxs)("ul",{class:"pagination justify-content-center",children:[e,Object(f.jsx)("li",{class:"page-item",children:Object(f.jsx)("input",{type:"button",value:"Next",class:"page-link btn btn-secondary btn-sm",onClick:function(){t.setState((function(e){return{paginationValue:t.state.paginationValue+10}})),t.fetchEtlFlowJobs(t.state.paginationValue+10).then((function(e){var n=e.jobruns;console.log(n),t.setState((function(t){return{jobruns:n}}))}))}})})]})})]})}}]),n}(a.a.Component),v=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).state={metrics:[]},s}return Object(c.a)(n,[{key:"fetchEtlFlowMetrics",value:function(){var t=Object(h.a)(j.a.mark((function t(){var e,n,s,a;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/etlflow",e=localStorage.getItem("Authorization"),t.next=4,fetch("/api/etlflow",{method:"POST",headers:{content_type:"applcation/json",Authorization:e},body:JSON.stringify({query:"\n          query {\n            metrics {\n                active_jobs\n                active_subscribers\n                etl_jobs\n                cron_jobs\n                used_memory\n                free_memory\n                total_memory\n                max_memory\n                build_time\n              }\n           }\n          "})});case 4:return 403===(n=t.sent).status&&(console.log("user not found"),localStorage.removeItem("Authorization")),t.next=8,n.json();case 8:return s=t.sent,a=s.data,t.abrupt("return",a);case 11:case"end":return t.stop()}}),t)})));return function(){return t.apply(this,arguments)}}()},{key:"componentDidMount",value:function(){var t=this;this.fetchEtlFlowMetrics().then((function(e){var n=e.metrics;console.log(n),t.setState((function(t){return{metrics:n}}))}))}},{key:"render",value:function(){var t=this;return Object(f.jsxs)("div",{class:"container",children:[Object(f.jsx)("ul",{class:"pagination justify-content-center",children:Object(f.jsx)("button",{class:"btn btn btn-sm",type:"button ",onClick:function(){t.fetchEtlFlowMetrics().then((function(e){var n=e.metrics;console.log(n),t.setState((function(t){return{metrics:n}}))}))},children:"Refresh"})}),Object(f.jsxs)("ul",{class:"list-group ",children:[Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Active Jobs"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.active_jobs})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Active Subscribers"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.active_subscribers})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Etl Jobs"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.etl_jobs})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Cron Jobs"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.cron_jobs})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Used Memory"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.used_memory})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Free Memory"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.free_memory})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Total Memory"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.total_memory})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Max Memory"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.max_memory})]}),Object(f.jsxs)("li",{class:"list-group-item d-flex justify-content-between align-items-center",children:[Object(f.jsx)("b",{children:"Build Time"}),Object(f.jsx)("span",{class:"badge badge-secondary badge-pill",children:this.state.metrics.build_time})]})]})]})}}]),n}(a.a.Component),g=n(19),y=n(2),w=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).state={jobs:[],metrics:[]},s}return Object(c.a)(n,[{key:"fetchEtlFlowJobs",value:function(){var t=Object(h.a)(j.a.mark((function t(){var e,n,s,a;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/etlflow",e=localStorage.getItem("Authorization"),t.next=4,fetch("/api/etlflow",{method:"POST",headers:{content_type:"applcation/json",Authorization:e},body:JSON.stringify({query:"\n          query {\n              jobs {\n                name \n                schedule\n                props {key value} \n                nextSchedule \n                schduleRemainingTime \n                is_active \n                job_deploy_mode \n                max_active_runs\n             }\n           }\n          "})});case 4:return 403===(n=t.sent).status&&(console.log("user not found"),this.remove_token()),t.next=8,n.json();case 8:return s=t.sent,a=s.data,t.abrupt("return",a);case 11:case"end":return t.stop()}}),t,this)})));return function(){return t.apply(this,arguments)}}()},{key:"setInterval",value:function(t){function e(){return t.apply(this,arguments)}return e.toString=function(){return t.toString()},e}((function(){var t=this;this.timerID=setInterval((function(){return t.fetchEtlFlowJobs()}),1e6)}))},{key:"componentDidMount",value:function(){var t=this;this.fetchEtlFlowJobs().then((function(e){var n=e.jobs;console.log(n),t.setState((function(t){return{jobs:n}}))}))}},{key:"remove_token",value:function(){localStorage.removeItem("Authorization"),this.props.doLogout("")}},{key:"render",value:function(){var t=this;return Object(f.jsx)(g.a,{children:Object(f.jsxs)("div",{class:"container-fluid",children:[Object(f.jsxs)("nav",{className:"navbar navbar-expand-lg navbar-dark bg-dark mb-3",children:[Object(f.jsxs)("ul",{className:"nav navbar-nav w-100",children:[Object(f.jsxs)("li",{className:"nav-item nav-link",children:[Object(f.jsx)(g.b,{to:"/",children:"Etlflow"})," "]}),Object(f.jsxs)("li",{className:"nav-item nav-link",children:[Object(f.jsx)(g.b,{to:"/JobRuns",children:"JobRuns"})," "]}),Object(f.jsxs)("li",{className:"nav-item nav-link",children:[Object(f.jsx)(g.b,{to:"/getInfo",children:"Metrics"})," "]})]}),Object(f.jsx)("div",{class:"navbar-collapse collapse w-100 order-3 dual-collapse2",children:Object(f.jsx)("ul",{class:"nav justify-content-end w-100",children:Object(f.jsxs)("li",{class:"nav-item dropdown",children:[Object(f.jsx)("a",{class:"nav-link  dropdown-toggle",href:"#","data-toggle":"dropdown",children:this.props.user_name}),Object(f.jsxs)("ul",{class:"dropdown-menu",children:[Object(f.jsx)("li",{children:Object(f.jsx)("a",{class:"dropdown-item",href:"https://github.com/tharwaninitin/etlflow",children:"Git Hub"})}),Object(f.jsx)("li",{children:Object(f.jsx)("a",{class:"dropdown-item",href:"https://tharwaninitin.github.io/etlflow/site/",children:"Docs"})}),Object(f.jsx)("li",{children:Object(f.jsx)("a",{class:"dropdown-item",children:Object(f.jsx)("input",{type:"button",class:"btn btn-secondary btn-sm",value:"LogOut",onClick:function(){return t.remove_token()}})})})]})]})})})]}),Object(f.jsxs)(y.c,{children:[Object(f.jsx)(y.a,{exact:!0,path:"/",component:function(){return Object(f.jsx)(O,{jobs:t.state.jobs})}}),Object(f.jsx)(y.a,{path:"/JobRuns",component:function(){return Object(f.jsx)(_,{jobs:t.state.jobs})}}),Object(f.jsx)(y.a,{path:"/getInfo",component:function(){return Object(f.jsx)(v,{})}})]})]})})}}]),n}(a.a.Component),k=n(34),S=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).state={user_name:"",password:"",token:"",login:[]},s.login_validation=s.login_validation.bind(Object(l.a)(s)),s.set_login_user_name=s.set_login_user_name.bind(Object(l.a)(s)),s.set_login_password=s.set_login_password.bind(Object(l.a)(s)),s}return Object(c.a)(n,[{key:"fetchLoginDetails",value:function(){var t=Object(h.a)(j.a.mark((function t(e,n){var s,a,i;return j.a.wrap((function(t){for(;;)switch(t.prev=t.next){case 0:return"/api/login",t.next=3,fetch("/api/login",{method:"POST",headers:{content_type:"applcation/json"},body:JSON.stringify({query:'\n          mutation {\n            login (user_name: "'.concat(e,'", password: "').concat(n,'") {\n                message \n                token\n             }\n           }\n          ')})});case 3:return s=t.sent,t.next=6,s.json();case 6:return a=t.sent,i=a.data,t.abrupt("return",i);case 9:case"end":return t.stop()}}),t)})));return function(e,n){return t.apply(this,arguments)}}()},{key:"login_validation",value:function(){var t=this;this.fetchLoginDetails(this.state.user_name,this.state.password).then((function(e){var n=e.login,s=JSON.parse(JSON.stringify(n)).token;if(""!==s){console.log("Valid user"),localStorage.setItem("Authorization",s);var a=JSON.parse(JSON.stringify(Object(k.a)(s)));t.props.doLogin(s,a.user),t.setState((function(t){return{token:s}}))}else console.log("Invalid User"),alert("Invalid User")}))}},{key:"set_login_user_name",value:function(){this.setState((function(t){return{user_name:document.getElementById("user_name").value}}))}},{key:"set_login_password",value:function(){this.setState((function(t){return{password:document.getElementById("password").value}}))}},{key:"render",value:function(){var t=this;return Object(f.jsxs)("div",{className:"main",align:"center",children:[Object(f.jsx)("nav",{className:"navbar navbar-expand-lg navbar-dark bg-dark mb-3",children:Object(f.jsx)("ul",{className:"nav navbar-nav w-50",children:Object(f.jsx)("li",{className:"nav-item nav-link",children:"EtlFlow"})})}),Object(f.jsx)("div",{className:"col-md-3",children:Object(f.jsx)("div",{className:"login-form ",children:Object(f.jsxs)("form",{id:"form_login",className:"text-center border border-dark p-5",onSubmit:function(e){t.login_validation(),e.preventDefault()},children:[Object(f.jsx)("p",{className:"h4 mb-4",children:"EtlFlow Login"}),Object(f.jsx)("div",{className:"form-group",children:Object(f.jsx)("input",{type:"text",className:"form-control","aria-describedby":"UsernameHelp",placeholder:"Username",id:"user_name",onChange:function(e){return t.set_login_user_name()}})}),Object(f.jsx)("div",{className:"form-group",children:Object(f.jsx)("input",{type:"password",className:"form-control","aria-describedby":"PasswordHelp",placeholder:"Password",id:"password",onChange:function(e){return t.set_login_password()}})}),Object(f.jsx)("button",{type:"submit",className:"btn btn-secondary btn-block",children:"Submit"})]})})})]})}}]),n}(a.a.Component),N=function(t){Object(d.a)(n,t);var e=Object(b.a)(n);function n(t){var s;return Object(r.a)(this,n),(s=e.call(this,t)).state={token:"",user:""},s.doLogin=s.doLogin.bind(Object(l.a)(s)),s.doLogout=s.doLogout.bind(Object(l.a)(s)),s}return Object(c.a)(n,[{key:"doLogin",value:function(t,e){this.setState({token:t,user:e})}},{key:"doLogout",value:function(t){this.setState({token:""})}},{key:"render",value:function(){return null===localStorage.getItem("Authorization")?Object(f.jsx)("div",{children:Object(f.jsx)(S,{doLogin:this.doLogin})}):Object(f.jsx)("div",{children:Object(f.jsx)(w,{doLogout:this.doLogin,user_name:this.state.user})})}}]),n}(a.a.Component);window.login_url="/api/login",window.url="/api/etlflow",o.a.render(Object(f.jsx)(a.a.StrictMode,{children:Object(f.jsx)(N,{})}),document.getElementById("root"))}},[[47,1,2]]]);
//# sourceMappingURL=main.5385fa96.chunk.js.map