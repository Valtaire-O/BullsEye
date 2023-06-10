import re
from w3lib.html import remove_tags, replace_escape_chars

class validations:
    def get_base_url(self,url):
        base = re.compile(r'^((?:https?:\/\/(?:www\.)?|www\.)[^\s\/]+\/?)').findall(url)
        if base:
            return base[0]
        return None

    def bad_prefix(self,url):
        return re.compile(r'^/|^//', flags=re.IGNORECASE).findall(url)

    def confirm_url(self,url: str, main_url=None):

        """Confirm the links are valid"""
        if url is None or type(url)!= str:
            return None

        if 'base64' in url:
            return None
        no_spaces = url.strip().split(' ')

        if len(no_spaces) > 1:
            """There should be no spaces in the links, if there are, check to see if its a list of 
            valid urls and take the first one, if not see if its a domain and page link and join it"""
            get_url = [self.get_base_url(i) for i in no_spaces if self.get_base_url(i)]
            size =len(get_url)
            if size > 1:
                # indicates a 'list' of n urls
                url = no_spaces[0]

            elif size ==1:
                'indicates only the first url is valid'
                url = ''.join(no_spaces)

            else:
                return None
        """if link is absolute, confirm its domain"""
        absolute = self.get_base_url(url)
        if absolute:
            valid_url = url.replace(absolute, absolute.lower())
            if re.compile(r'^www').findall(valid_url):
                return  'https://'+valid_url
            # make sure that the domain name has no uppercase, otherwise it will be changed by the browser
            return valid_url

        if main_url:
            """For relative paths, get the base url from the main url"""
            base_url = self.get_base_url(main_url)
            # make sure theres no slashes at the start of the path
            if self.bad_prefix(url):
                new_url = url.split('/')
                clean_url = [i for i in new_url if i]
                # if the base url has no slash add one to make a valid url
                if base_url[-1] != '/':
                    base_url = base_url + '/' + '/'.join(clean_url)
                else:
                    base_url = base_url + '/'.join(clean_url)

            else:
                base_url = base_url + url
            no_spaces = replace_escape_chars(base_url).strip().split(' ')

            if len(no_spaces) > 1:
                # print(base_url)
                return None
            if re.compile(r'^www').findall(base_url):
                return  'https://'+base_url
            return base_url

    def image_file_type(self,url):
        if not url:
            return None
        image_types = re.compile(rf'(?i)(png|svg|logo|jpg|jpeg|gif|webp|image)', flags=re.IGNORECASE).findall(url)
        if image_types:
            return url
        return None


'''check = validations().confirm_url('www.CarlsonFoley.com')
print(check)'''
'''test = ["columbus.org/membership",
"calendarwiz.com/calendars/calendar.php?crd=gcvacommunitycalendar&cid%5B%5D=all&jsenabled=1&winh=937&winw=960&inifr=false",
"caar.com/membership/realtor-(primary-secondary)",
"toryburchfoundation.org/subscribe/",
"lisc.org/virginia/regional-stories/?localCategory=lisc-va-stories",
"theautry.org/events/signature-programs/american-indian-arts-marketplace",
".http://mips.umd.edu/",
"Robins Air Force Base:  Project Synergy",
"https://www.worldcampus.psu.edu /degrees-and-certificates/penn-state-online-earth-sustainability-certificate/overview",
"https://extension.purdue.edu/Jackson, https://extension.purdue.edu/",
"https://extension.purdue.edu/, https://extension.purdue.edu/Vigo",
"middlesex.njaes.rutgers.edu",
"gaebler.com",
"phdcincubator.org/incubator",
"prairietownship.org",
"compostcrew.com",
"https://",
"https://",
"https://",
"fullerlife0andp.com",
"BikeandBrunchTours.com",
"1Password.com",
"Chekr.com",
"703-282-0071",
]
validate = validations()
for t in test:
    check = validate.confirm_url(t)
    if not check:
        print(t)'''